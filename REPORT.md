# Blood Bank Management and Inventory Analytics Using Apache Spark

## 1. Project title

Blood Bank Management and Inventory Analytics Using Apache Spark. Big Data Analytics mini project. Add student, department, institution, guide and academic year.

## 2. Abstract

This mini project develops a local blood donation analytics and inventory demonstration system using Apache Spark. Its main analytical source is a predefined public dataset published by Malaysia's Ministry of Health through data.gov.my. The included snapshot contains 493,220 daily state and blood group observations. Python validates source fields, checks duplicate observations, records provenance and stores typed data in Parquet format. Spark SQL performs monthly, yearly and daily aggregations, blood group distributions, state comparisons and weekday summaries. Streamlit provides interactive filters, tables and charts, with Pandas and Plotly supporting presentation. Published subtotal rows are handled separately from blood group categories to prevent double counting. A source-total discrepancy is preserved and disclosed. Because the public dataset does not include individual donors, requests or expiration dates, those workflows remain a clearly labeled synthetic demonstration. Operational changes use validation, local writer locks and immutable snapshots published through one manifest pointer. Tests check data integrity, request fulfillment, failure recovery and interface behavior. The project demonstrates practical ingestion, columnar storage, SQL aggregation and execution-plan inspection without claiming a distributed deployment or suitability for clinical decisions.

## 3. Introduction

Blood-bank information involves different data grains. Published daily totals describe collection activity, while individual unit records support operational inventory. Combining them without distinguishing their meanings would produce misleading stock estimates. The project separates these two scopes while demonstrating a Spark-based analytical workflow.

## 4. Problem statement

Analyze a licensed predefined blood-donation dataset with explainable queries and preserve a simple management demonstration. Prevent subtotal double-counting, invalid inputs, duplicate identifiers, expired issuance and inconsistent request/inventory publication. Replace the original MongoDB backend without falsely describing Spark as a transactional database.

## 5. Objectives

- Validate, preserve and import the official dataset into Parquet.
- Execute the main analytical pipelines in Spark SQL.
- Show the existing interactive charts and filters.
- Retain donor, unit, request, FEFO and alert demonstrations using synthetic data.
- Inspect actual physical plans and record test evidence.
- Document source coverage, discrepancies and local-storage limitations.

## 6. Existing system and limitations

The previous project used MongoDB documents, aggregation pipelines and atomic unit updates. It was appropriate for a small operational application but did not satisfy the revised requirement to demonstrate Spark or Hadoop. A hypothetical manual register is also less convenient for repeated aggregation and consistency checks; this comparison is an educational assumption, not a hospital survey.

## 7. Proposed system

Use Apache Spark SQL locally over Parquet tables. Retain five Streamlit pages and the real/simulated distinction. Manage small operational updates with Python validation, thread and local OS file locks, immutable files and atomic manifest replacement. Keep old MongoDB data unchanged for preservation; it is not used by the active application.

## 8. Technology stack

| Technology | Role |
|---|---|
| Python | Validation, orchestration, operational logic and tests |
| PySpark 4.0.1 / Java 21 | Local Spark SQL execution |
| Parquet | Persistent typed columnar storage |
| PyArrow | Portable Parquet writes and operational snapshot reads |
| Streamlit | Interactive application |
| Pandas | CSV parsing, validation and result formatting |
| Plotly | Charts |
| Faker | Optional synthetic management records |
| python-dotenv | Environment configuration |
| unittest / Streamlit AppTest | Executable QA |

PyMongo is an optional dependency of the one-time read-only migration script only.

## 9. System architecture

Official CSV → validation/provenance → Parquet public snapshot → Spark SQL → aggregated results → Streamlit/Plotly.

Management form → validated Python operation → local writer lock → new immutable operational files → atomic current.json replacement → Spark reports.

Spark runs with local[2], meaning local worker threads, not a two-machine cluster. Main analytical computations execute in Spark. Initial CSV preparation and small record mutations are intentionally driver-side, not falsely described as distributed ETL.

## 10. Storage design

The public namespace contains daily_donations: date, state, blood_type, donations, source_row and source_sha256. Natural key: date/state/blood type. No individual donor identities are supplied.

The management namespace contains donors, donations and blood_requests. Fields and one-to-many relationships remain consistent with the original workflows. IDs are strings rather than BSON ObjectIds. UTC microsecond timestamps and 64-bit integer quantities are explicitly typed. Full schemas are in DATABASE_DESIGN.md.

Each namespace has a current manifest mapping tables to immutable version files. Old files are retained for pinned readers and recovery. There are no native Parquet unique indexes; Python validation enforces uniqueness under the writer lock.

## 11. Modules

Dashboard: real donation analytics or simulated stock KPIs/charts. Donor Management: register/search synthetic donors. Blood Inventory: record units, inspect stock and expiry. Blood Requests: add exact-group demand and fulfill with FEFO. Analytics: SQL text, output tables, storage statistics and Spark physical plans.

The public report is organized into Overview, Trends & patterns, and Source & data tabs. It uses consistent chart colors, compact metrics and selection-based descriptive findings. Partial calendar periods are explicitly marked. Daily smoothing requires consecutive dates, weekday averages normalize by observed dates, and filtered CSV summaries retain their selection context. Bounded immutable-snapshot caching reduces repeated query work without replacing Spark aggregation.

The public loader validates and verifies source data. The seed generator builds optional reproducible test data. The migration script preserves old records without requiring MongoDB in the active application.

## 12. Big Data techniques

Columnar storage, bulk snapshot ingestion, typed schemas, Spark SQL filtering/grouping/summing, shuffle exchanges, predicate pushdown, column pruning and EXPLAIN FORMATTED are demonstrated.

Volume: 493,220 aggregate observations. Variety: time, categories, counts and operational table structures. Velocity: daily publisher updates and controlled snapshot replacement, not a streaming implementation. No million-document or multi-machine throughput claim is made; 9,896,044 is the sum of recorded counts.

## 13. Analytical queries

The public pipelines summarize monthly, yearly, daily, state, ABO and weekday activity. Overall trends select blood_type='all'; group distribution excludes subtotal rows. SUM(donations) measures donated quantities; COUNT(*) measures source observations.

Operational SQL covers available stock, donation trends/distribution, requested quantities/trends, top cities, expired/near-expiry counts and effective statuses. A LEFT JOIN derives outstanding requests from linked issued units. See AGGREGATIONS.md for executable SQL and explanations.

## 14. Implementation

The source CSV remains unchanged. Parsing rejects unexpected columns/categories, invalid dates/counts, duplicates and incomplete category groups. Source hash and row number support provenance. The published total exceeds the ABO breakdown by 306 across 285 date/state observations; this is disclosed, not altered.

FEFO selects the earliest-expiring unissued exact-group unit, with unit ID as tie-breaker. The issue amount cannot exceed demand or eligible stock. Host time is checked before selecting and recording issue timestamps. Available stock is computed from unit records.

The local snapshot protocol writes and flushes new files before atomically publishing one pointer. Inventory and request summaries are visible together. A failed pre-publication save leaves prior data intact; an uncertain acknowledgement requires checking the committed snapshot before retry. This is a local application mechanism, not a distributed ACID storage engine.

## 15. Testing and results

The final dashboard regression suite passed 77 tests with real Spark integration enabled on 29 October 2026. It covers business rules, source validation, SQL totals, filters, unique identifiers, file-save failure injection, uncertain acknowledgement, competing writers, Streamlit forms, cache invalidation and analytical-presentation safeguards. Browser checks additionally inspect the actual real-data interface.

All 493,220 public rows were compared with Parquet successfully. The original 2,500 donors, 5,000 units and 1,000 requests were migrated and compared record-by-record; MongoDB was not modified. Detailed measured results, additional full-dataset checks, scale checks and warnings are recorded in VERIFICATION.md rather than invented in this report. Rerun tests for changed code or a different machine.

## 16. Advantages

The project satisfies the revised Spark requirement while retaining the familiar interface and source dataset. SQL plans are visible, source limitations are transparent, operational changes are validated before publication, and previous source/data are preserved. No unofficial Windows Hadoop executable is needed for the verified read/write path.

## 17. Limitations

Spark is not an OLTP database. Management writes rewrite small tables and keep older versions, increasing disk usage. The snapshot protocol assumes local disk and cooperating writers, not HDFS, shared storage or network synchronization. Authentication, clinical checks, distributed transactions and automatic version cleanup/recovery are absent.

The public source covers selected collection sites, lacks Rh and unit-level information, has a small total discrepancy and includes an incomplete latest year. It cannot establish real current blood inventory. Java startup and Spark job overhead may be noticeable for small queries. Correct host time is required. Testing does not establish clinical safety or absence of every bug.

## 18. Future enhancements

Potential work includes reviewed source refreshes, partitioning studies, measured caching improvements, read-only exports and explicit snapshot retention/recovery tools. If a genuine distributed transactional workflow is required, evaluate an appropriate transactional table/storage system with additional tests. These are future work, not implemented features.

## 19. Conclusion

The application demonstrates predefined-data ingestion, Parquet storage, Spark SQL analytics and a separately labeled inventory simulation. It preserves the key interface and business workflows while replacing the active MongoDB backend. Its main educational value is the connection between data meaning, query execution, consistency safeguards and honest verification.

## 20. References

- [Apache Spark 4.0.1 overview](https://spark.apache.org/docs/4.0.1/)
- [Spark SQL Parquet](https://spark.apache.org/docs/4.0.1/sql-data-sources-parquet.html)
- [Spark EXPLAIN](https://spark.apache.org/docs/4.0.1/sql-ref-syntax-qry-explain.html)
- [Apache Arrow Python Parquet](https://arrow.apache.org/docs/python/parquet.html)
- [Official dataset](https://data.gov.my/data-catalogue/blood_donations_state)
- [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- [Streamlit documentation](https://docs.streamlit.io/)
- [Plotly Python](https://plotly.com/python/)

Updated for the Spark migration on 29 October 2026. Historical MongoDB materials are retained only in the backup directory.
