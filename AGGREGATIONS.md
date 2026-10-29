# Apache Spark SQL pipelines

All main dashboard summaries now execute with Spark SQL over Parquet temporary views. MongoDB aggregation dictionaries and index comparisons were removed from the active implementation.

## Run the exact application queries

~~~python
from database import connect
from public_data import public_pipelines, public_report, explain_public
db = connect('public')
for name, sql in public_pipelines().items():
    print(name, sql)
    print(db.query(sql))
print(public_report(db)['total'])
print(explain_public(db)['physical_plan'])
~~~

The public importer must run first. All groups, dates and state selections are validated; text literals are escaped. The interface does not execute user-supplied arbitrary SQL.

## Public analytics: six working queries

~~~sql
-- 1. Monthly donations: SUM counts, do not count CSV rows.
SELECT date_format(date, 'yyyy-MM') AS month, SUM(donations) AS count
FROM daily_donations
WHERE blood_type = 'all'
GROUP BY date_format(date, 'yyyy-MM')
ORDER BY month;

-- 2. Annual donations (latest year is partial).
SELECT date_format(date, 'yyyy') AS year, SUM(donations) AS count
FROM daily_donations WHERE blood_type = 'all'
GROUP BY date_format(date, 'yyyy') ORDER BY year;

-- 3. Daily donations across selected collection states.
SELECT date_format(date, 'yyyy-MM-dd') AS day, SUM(donations) AS count
FROM daily_donations WHERE blood_type = 'all'
GROUP BY date_format(date, 'yyyy-MM-dd') ORDER BY day;

-- 4. States ranked by recorded quantity (not population-adjusted rates).
SELECT state, SUM(donations) AS count
FROM daily_donations WHERE blood_type = 'all'
GROUP BY state ORDER BY count DESC, state LIMIT 13;

-- 5. ABO distribution EXCLUDES published subtotal rows.
SELECT upper(blood_type) AS blood_group, SUM(donations) AS count
FROM daily_donations WHERE blood_type IN ('a', 'b', 'ab', 'o')
GROUP BY blood_type ORDER BY blood_type;

-- 6. Weekday totals, Monday = 1. These are totals, not per-day averages.
SELECT ((dayofweek(date) + 5) % 7) + 1 AS weekday, SUM(donations) AS count
FROM daily_donations WHERE blood_type = 'all'
GROUP BY ((dayofweek(date) + 5) % 7) + 1 ORDER BY weekday;
~~~

WHERE selects rows; GROUP BY creates buckets; SUM adds recorded quantities; ORDER BY sorts; LIMIT bounds output. date_format creates date labels. The app adds inclusive date/state/group filters. Inclusive end dates are represented by "less than the following midnight". Published totals exceed the ABO sum by 306 in the included snapshot; raw values and the disclosure are retained.

## Management analytics

analytics.pipelines() returns SQL for:
- Available units by group: filter Available, no request link, donated by now, expiry after now; COUNT(*).
- Monthly simulated donations: date_format + COUNT(*), because each document-equivalent row is one unit.
- Donation distribution: GROUP BY blood_group + COUNT(*).
- Requested quantities: GROUP BY blood_group + SUM(units_requested).
- Monthly demand: request-month buckets + SUM(units_requested).
- Top cities: donor GROUP BY city + COUNT(*) + LIMIT 10.
- Expired unissued: COUNT(*) where expiry <= now and status is not Issued.
- Near expiry: available predicate plus expiry <= now + configured days.
- Status distribution: CASE preserves Issued, classifies elapsed unissued units as Expired, then GROUP BY.
- Pending KPI: LEFT JOIN requests to counts of issued units, COALESCE missing counts to zero, count requests with outstanding demand.

Historical date filters do not pretend to reconstruct past stock. Current inventory/expiry remains current; public-view filters apply to its entire report.

## Explain and timing

~~~powershell
python analytics.py --explain --group 'O+'
~~~

The Analytics page also calls EXPLAIN FORMATTED for the public and inventory SQL. Look for Parquet Scan/FileScan, ReadSchema (column projection), PushedFilters, HashAggregate, Sort and Exchange (shuffle). The exact plan is printed, not invented. An elapsed time and returned-result count describe one actual run; there is no index benchmark and no claimed universal speedup.

Spark transformations are planned lazily; an action such as collect executes the job. Only aggregated outputs or 100-row pages normally return to the interface. ETL validation and small operational writes use Python/Pandas/PyArrow; this is not falsely presented as fully distributed ETL.

## Big Data concepts

Volume: 493,220 aggregate source records, not 9.9 million stored donor/unit records. Variety: timestamps, categories, counts and separate operational schemas. Velocity: the publisher's daily data and controlled snapshot reimports, not live streaming. Spark runs locally with two threads; no HDFS/YARN cluster or multi-machine performance was tested.
