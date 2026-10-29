# Spark SQL / Parquet storage design

The active application has no MongoDB collections. Spark is the query engine; persistent data is in local Parquet files with explicit Arrow schemas in database.py. No managed Hive catalogue, HDFS cluster or Delta Lake is required.

## Layout

~~~text
data/spark/
  public/
    current.json
    .write.lock
    versions/<uuid>/daily_donations.parquet
    versions/<uuid>/manifest.json
  management/
    current.json
    .write.lock
    versions/<uuid>/donors.parquet
    versions/<uuid>/donations.parquet
    versions/<uuid>/blood_requests.parquet
    versions/<uuid>/manifest.json
~~~

The current manifest maps logical table names to immutable files. A query pins that mapping and registers temporary views in its own Spark SQL session. It does not read whichever directory was most recently created. Old versions remain intentionally; a failed/unpublished version is not active.

## Types and required fields

All IDs are strings. Existing MongoDB ObjectIds are converted to their string representation during read-only migration; new records use UUID strings. Numeric fields use 64-bit integers. Timestamps use UTC microseconds; source date-only values become UTC midnight. Parquet columns may physically support null, but the application validator enforces business-required values before publishing.

### donors

| Field | Type | Rule |
|---|---|---|
| _id, donor_id | string | Unique and nonempty; readable donor ID validated |
| name, city | string | Required validated text |
| age | integer | 18–65 demonstration range |
| gender | string | Supported option |
| blood_group | string | One of eight supported ABO/Rh groups |
| contact_number | string | Ten ASCII digits; synthetic use only |
| registration_date | timestamp | Not future |

### donations

| Field | Type | Rule |
|---|---|---|
| _id, donation_id, unit_id | string | Unique within their identifier fields |
| donor_id | string | Existing donor |
| blood_group | string | Matches donor |
| donation_date | timestamp | On/after registration, not future |
| expiration_date | timestamp | After donation; new records use configured 42-day shelf life |
| unit_status | string | Available / Issued / Expired |
| request_id | nullable string | Valid exact-group request when issued; otherwise null |
| issued_at | nullable timestamp | After donation/request, before expiry; otherwise null |

### blood_requests

| Field | Type | Rule |
|---|---|---|
| _id, request_id | string | Unique |
| hospital_name | string | Required synthetic text |
| blood_group | string | One of eight supported groups |
| units_requested | integer | 1–10,000 |
| units_fulfilled | integer | Equals linked issued units, never exceeds demand |
| request_date | timestamp | Not future |
| request_status | string | Pending / Partially Fulfilled / Fulfilled; agrees with quantities |

Relationships: donor → many units; request → many issued units. One unit cannot be linked to two requests. Stock has no separate counter.

### daily_donations (public namespace)

date (UTC timestamp), state (string), blood_type (a/b/ab/o/all), donations (nonnegative integer), source_row (integer), source_sha256 (string). Natural key: date/state/blood_type. Source CSV validation checks its uniqueness and complete categories. No donor or unit relationship is fabricated. See [DATASET.md](DATASET.md).

## Consistency protocol

1. Acquire the thread lock and local OS file lock.
2. Read the latest committed manifest and operational tables.
3. Apply the requested change in memory.
4. Validate identifiers, references, quantities and dates.
5. Write new Parquet files and flush writable handles.
6. Save the version manifest, then atomically replace current.json.
7. Release the lock.

All three management tables are published through one pointer, preventing mixed old/new request and inventory versions. Repeated fulfillment reads committed linked units before issuing. If acknowledgement is uncertain, inspect the active snapshot before retrying. No native Parquet row transactions or unique indexes are claimed.

## Query efficiency

Spark projects required columns, applies filters, aggregates and shuffles as needed. Parquet row-group statistics can support predicate pushdown; inspect EXPLAIN FORMATTED for the actual plan. There are no B-tree/MongoDB indexes. Public Parquet is written with Snappy compression and 100,000-row groups, not partitioned by state/year. This is intentionally simple local storage.

## Limits and recovery

Local filesystem only; one normal application process. Advisory locks do not stop external programs that ignore them. No automated version garbage collection, authentication, HDFS deployment or disaster-recovery manager is included. Back up whole namespaces. A version manifest can reference older version files, so copying only its own folder may be insufficient. Stop readers/writers before restoring a known-good manifest with all referenced data. Old MongoDB data and original source backups remain untouched.
