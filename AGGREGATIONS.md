# MongoDB analytics walkthrough

All main chart calculations execute in MongoDB. Python receives small result sets; Pandas formats them for Plotly. The source of the pipelines is `analytics.pipelines()`, so the examples below execute the same queries as the dashboard. `$match` reduces input, `$group` combines similar documents, `$sum` counts or adds quantities, `$sort` orders, `$project` selects/renames fields, `$count` totals matches, `$dateToString` builds month labels, and `$limit` restricts output.

## Run the actual queries

After seeding, start Python from the activated project environment:

```powershell
python
```

Run this block:

```python
from pprint import pprint
from database import connect
from analytics import pipelines

client, db = connect()
queries = pipelines()
for name in ('available_by_group', 'monthly_donations', 'requested_by_group',
             'top_cities', 'expired_count', 'near_expiry_count', 'inventory_status'):
    collection, pipeline = queries[name]
    print(name)
    pprint(list(db[collection].aggregate(pipeline)))
client.close()
```

These are seven executable aggregations. The Analytics page also displays their complete pipeline JSON and live results. An empty `$count` result is `[]`, which the application interprets as zero.

## Query 1: available units by blood group

```python
from config import utcnow
pipeline = [
    {'$match': {'unit_status': 'Available', 'request_id': None,
                'expiration_date': {'$gt': utcnow()}}},
    {'$group': {'_id': '$blood_group', 'count': {'$sum': 1}}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$project': {'_id': 0, 'blood_group': '$_id', 'count': 1}},
]
# list(db.donations.aggregate(pipeline)) while the connection is open
```

First keep only unissued, unexpired Available units. Then count documents in each group. Sort by stock and return readable field names. With All selected, Python fills missing groups with zero so shortages are visible for all eight groups. Selecting a blood group shows only that group; excluded groups are not mislabeled as having zero stock.

## Query 2: monthly donations

```python
pipeline = [
    {'$match': {}},
    {'$group': {'_id': {'$dateToString': {'format': '%Y-%m',
                                        'date': '$donation_date', 'timezone': 'UTC'}},
                'count': {'$sum': 1}}},
    {'$sort': {'_id': 1}},
    {'$project': {'_id': 0, 'month': '$_id', 'count': 1}},
]
```

Turn each donation timestamp into its UTC year-month, count donations for each month, and sort chronologically. Available, Issued and Expired donations all contribute because they were all received. Date/group filters go in `$match`. Pandas fills missing months *between observed months* with zeros when plotting, without changing MongoDB's result.

## Query 3: requested units by blood group

```python
pipeline = [
    {'$match': {}},
    {'$group': {'_id': '$blood_group', 'count': {'$sum': '$units_requested'}}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$project': {'_id': 0, 'blood_group': '$_id', 'count': 1}},
]
```

Run on `blood_requests`. Summing `units_requested` measures units of demand, not the number of request documents. A ten-unit request contributes ten. Fulfilled requests still belong in historical demand analysis. `monthly_requests` similarly groups by request month and adds requested units.

## Query 4: top donor cities

```python
pipeline = [
    {'$match': {}},
    {'$group': {'_id': '$city', 'count': {'$sum': 1}}},
    {'$sort': {'count': -1, '_id': 1}},
    {'$project': {'_id': 0, 'city': '$_id', 'count': 1}},
    {'$limit': 10},
]
```

Run on `donors`. Count registered donors by city, sort largest first, and return only the top ten. A donor with multiple donations still counts once. An enabled historical filter refers to the donor's registration date.

## Query 5: expired and near-expiry counts

```python
from datetime import timedelta
from config import NEAR_EXPIRY_DAYS, utcnow
now = utcnow()
expired_pipeline = [
    {'$match': {'unit_status': {'$ne': 'Issued'},
                'expiration_date': {'$lte': now}}},
    {'$count': 'count'},
]
near_pipeline = [
    {'$match': {'unit_status': 'Available', 'request_id': None,
                'expiration_date': {'$gt': now,
                                    '$lte': now + timedelta(days=NEAR_EXPIRY_DAYS)}}},
    {'$count': 'count'},
]
```

Run both on `donations`. Expired units include stale Available records whose date has elapsed, but exclude issued history. Near expiry is an inclusive seven-day upper boundary with an exclusive current-time boundary. The same predicates return the actual units for the alert tables.

## Query 6: inventory status distribution

```python
from config import utcnow
pipeline = [
    {'$match': {}},
    {'$project': {'status': {'$switch': {'branches': [
        {'case': {'$eq': ['$unit_status', 'Issued']}, 'then': 'Issued'},
        {'case': {'$lte': ['$expiration_date', utcnow()]}, 'then': 'Expired'},
    ], 'default': '$unit_status'}}}},
    {'$group': {'_id': '$status', 'count': {'$sum': 1}}},
    {'$sort': {'_id': 1}},
    {'$project': {'_id': 0, 'status': '$_id', 'count': 1}},
]
```

Calculate effective status before counting. Test Issued first to preserve history, then determine expiry from the timestamp. Thus counts remain accurate even before the periodic UI interaction marks elapsed units Expired. For valid application data, the three status counts sum to total donations.

## Other analytics

Donation blood-group distribution groups all donations and uses `$sum: 1`. Request status is derived by `$lookup` into donations and `$count` of linked Issued units. This lookup makes request views resilient to interrupted summary updates. The KPI labelled Pending Blood Requests counts both pending and partially fulfilled requests, because both still need units.

## Explain and index comparison

Run:

```powershell
python analytics.py --explain --group 'O+'
python analytics.py --explain --group 'O+' --raw
```

PyMongo's `Database.command()` wraps the aggregation in the server's `explain` command with `verbosity='executionStats'`. The first execution hints `{'$natural': 1}` to force a collection scan. The second ensures indexes exist and hints `inventory_fefo` to use the compound index. This safely compares access paths without dropping unique constraints or changing existing data. When indexes already exist, the first execution is a **simulated no-index access path**, not a literal measurement before index creation.

The first `$match` selects blood group, Available status and unexpired units. The compound index narrows the candidate set by those fields. `request_id: null` is verified against candidate documents. The grouped output remains the same.

| Statistic | Meaning |
|---|---|
| `executionTimeMillis` | Server execution time for that explain run; may be 0 for small queries |
| `totalDocsExamined` | Number of documents inspected during query execution |
| `totalKeysExamined` | Number of index entries inspected |
| `nReturned` | Returned records at the reported execution layer; may be aggregate rows or cursor-stage documents depending on the plan |

The function supports top-level statistics and aggregation `$cursor` statistics. Missing statistics display as null rather than an invented number. Inspect the raw plan for `COLLSCAN` or `IXSCAN` and server-specific details. Time depends on hardware, cache state and query planning; one run does not establish a general speedup. Repeat if conducting a formal experiment, keep the dataset/query fixed and report the methodology. The actual local observation is in `VERIFICATION.md`.

Primary references: [Explain results](https://www.mongodb.com/docs/manual/reference/explain-results/), [measure index use and natural hints](https://www.mongodb.com/docs/manual/tutorial/measure-index-use/), and [explain command](https://www.mongodb.com/docs/manual/reference/command/explain/).

## Big Data Analytics concepts

**Volume:** configurable generation and bulk insertion let students compare 1,000–25,000 donation records plus related documents. These are demonstration sizes, not proof of industrial scale. Database-side aggregation avoids transferring every source document to the UI.

**Variety:** donors, donations and requests have distinct document structures, and contain strings, integers, dates, nulls and ObjectIds. This demonstrates varied structured records; it does not claim to process images, genomic data or unstructured streams.

**Velocity:** new donation and request writes change operational inventory and subsequent analytics. This is modest interactive updating, not a streaming system. Refreshing the app reruns queries; it does not continuously ingest external feeds.

Bulk insertion reduces network command overhead. Indexes reduce inspected candidates for selective queries. Aggregation produces descriptive summaries. Together these demonstrate techniques relevant to big data analytics on a small local dataset. The app uses one MongoDB server and no distributed processing.
