# Ten-slide presentation: Apache Spark edition

## 1. Title

Blood Bank Management and Inventory Analytics Using Apache Spark. Student, roll number, department and guide.

Say: “The project combines real public donation analytics with a separate synthetic inventory demonstration.”

## 2. Problem and objectives

Analyze donation patterns; avoid misleading totals; retain donor/unit/request workflows; prevent expired or duplicate allocation; demonstrate Spark SQL and columnar storage. Educational system, not a clinical product.

## 3. Dataset

Official Daily Blood Donations by Blood Group & State, Ministry of Health Malaysia, data.gov.my, CC BY 4.0. Included snapshot: 493,220 observations, 13 collection states, 2006-01-01 to 2026-10-10. Counts sum to 9,896,044 donations, not that many individual records.

## 4. Architecture and stack

CSV → Python validation → Parquet → Spark SQL → Streamlit/Plotly.

Python/Pandas for preparation; PyArrow for portable Parquet writes; PySpark + Java 21 for queries; Faker only for management simulation. Local[2] means two local threads. MongoDB is not an active dependency.

## 5. Storage and correctness

Public daily_donations table; separate management donors/donations/blood_requests. Typed timestamps/counts/string IDs. Readable unit/request links. Immutable versions plus one current manifest. Local thread/file lock and whole-state validation protect writes. Parquet itself has no unique indexes or row transactions.

## 6. Real-data analytical pipelines

Monthly/yearly/daily trends, ABO distribution, state totals and weekdays. WHERE → GROUP BY → SUM/COUNT → ORDER BY. Overall trends use only 'all'; ABO chart excludes 'all'. Published totals minus ABO = 306, disclosed rather than silently corrected.

The redesigned interface uses Overview, Trends & patterns, and Source & data tabs. Gold points/bars distinguish incomplete months/years. Weekday averages account for the number of represented weekdays; the daily rolling average requires seven consecutive observed dates. Descriptive findings follow the current filters, and summary CSV exports preserve filter context.

## 7. Management demonstration

Register synthetic donor → record unit → add request → FEFO allocation. Exact group only, 42-day demonstration shelf life, demand/stock limits. Available → Issued or Expired. Low stock below 5; near expiry within 7 days. These are not measured real stock values.

## 8. Spark execution and Big Data concepts

Show SQL and EXPLAIN FORMATTED. Discuss FileScan/Parquet, ReadSchema, PushedFilters, HashAggregate and Exchange. Main analytical work executes in Spark; small mutations remain Python. Volume/variety/controlled updates; no distributed benchmark or streaming claim.

## 9. QA and limitations

Show actual VERIFICATION.md results. Tests cover duplicates, FEFO, expiry, partial demand, source totals, failed publication, retries, local competing writers, filters and Streamlit. Limitations: local filesystem, snapshot rewrite overhead, retained-version disk growth, partial source coverage, no clinical/security workflow. Warnings and untested areas are disclosed.

## 10. Conclusion and live demo

Open real dashboard; filter state/date/ABO; show the discrepancy and SQL plan; switch clearly to simulation; demonstrate one request; show test evidence. Future work: retention/recovery tools and measured performance improvements.

# Twenty viva answers

1. **Why Spark now?** The revised course requirement asks for Spark/Hadoop. Spark SQL demonstrates analytical execution over Parquet while keeping the existing Python interface.
2. **Is Spark a database?** No. It is a processing engine. Parquet files provide persistent storage here.
3. **Where is the data stored?** In versioned Parquet files under data/spark, selected by current.json manifests.
4. **Is this Hadoop?** It is Apache Spark, using bundled Hadoop client libraries internally. No HDFS/YARN or Hadoop cluster is deployed.
5. **What is local[2]?** Two local worker threads, not two machines.
6. **What is Parquet?** A typed columnar file format that supports efficient projection, compression and row-group statistics.
7. **Why PyArrow?** It writes standard Parquet portably and reads small operational snapshots without unofficial Windows Hadoop helper binaries.
8. **What does Spark do?** It executes the main filtering, grouping, joins, summations, sorting and physical plans.
9. **What does Pandas do?** CSV validation/preparation and chart-output formatting, not the main Spark dashboard aggregation.
10. **What is an action?** An operation such as collect that triggers Spark's planned computation.
11. **What is a shuffle?** Data redistribution needed for operations such as grouping; physical plans can show Exchange.
12. **What is predicate pushdown?** Passing eligible filters toward the data scan so the source can avoid unnecessary reads where supported.
13. **What replaced MongoDB indexes?** Parquet/Spark scan optimizations, not equivalent unique indexes. Python validates uniqueness under the writer lock.
14. **How are duplicate units prevented?** Identifier validation before publishing the new state, while local writers are serialized.
15. **How is stock consistent?** Stock is derived from unit records; unit and request updates are published through one manifest replacement.
16. **Is this a distributed transaction?** No. It is a local filesystem snapshot protocol, not an ACID distributed storage engine.
17. **How does FEFO work?** Select available exact-group units by earliest expiry, then unit ID, without exceeding stock or demand.
18. **Why not sum every CSV row?** 'all' rows are subtotals; adding categories and totals double-counts donations.
19. **Why are inventory records synthetic?** The public source lacks unit IDs, Rh, issues, requests and expiry. Inventing these as real data would be misleading.
20. **What did QA find?** A Windows read-only flush error, import-corruption detection weakness and configuration-error handling risk were fixed; failure/retry/concurrency tests and Spark/UI checks provide evidence. Remaining limitations are documented rather than claimed away.

Use only measured results from VERIFICATION.md or a new run. Never present old MongoDB timings as Spark performance.
