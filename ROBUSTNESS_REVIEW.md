# Spark migration robustness review — 29 October 2026

## Scope and findings

The active backend is now Spark SQL with local Parquet storage. The five pages and data meanings remain. Old MongoDB source and database records are preserved. This review reports found issues and tested safeguards, not a proof that every possible bug has been eliminated.

| Finding / migration risk | Fix or mitigation | Evidence |
|---|---|---|
| Java 8 on PATH cannot run Spark 4.0.1 | Project-local Java 21, autodetected or SPARK_JAVA_HOME override | Actual Spark 4.0.1 SQL smoke run |
| Windows fsync on a read-only Parquet handle raises Bad file descriptor | Open the flushed handle as r+b | Reproduced failure; full 493,220-row import then succeeded |
| Parquet cannot replace MongoDB atomic row updates directly | Local thread + OS file lock; validate full state; publish inventory and requests with one pointer | FEFO, competing callers and failed-publication regressions |
| A successful publish can have an uncertain caller acknowledgement | Retry re-reads committed linked units; error message does not falsely promise no change | Injected failure after os.replace, retry issues no excess units |
| Old import shortcut trusted count and hash label | Repeat import now performs full CSV/content comparison | Modified same-size snapshot rejected |
| Invalid settings could raise before the friendly UI handler | Move storage/config import inside the guarded block | Invalid-configuration AppTest |
| Mutable views might collide across simultaneous reports | Query-local Spark sessions and immutable manifest paths | SQL/UI tests; old-snapshot read regression |
| Dropping MongoDB unique indexes removes duplicate protection | Validate all readable IDs and links before locked publication | Duplicate donor, unit, donation and request tests |
| User text must not become SQL syntax | Validated enums, escaped literals, literal city contains search | Quote/injection-like text regression |
| Missing/corrupt snapshot could look like an empty database | Fail closed for malformed manifests, escaped paths and missing referenced files | Corruption regressions |
| Data-source subtotals can double counts | Keep 'all' separate from ABO categories in Spark SQL | Six-query totals and real-snapshot comparison |
| Documentation could falsely describe the new engine | Replace MongoDB pipeline/index material with SQL/Parquet plans and local protocol | Updated README, schema, report, viva and source appendix |

## Executed checks

The initial complete Spark migration suite passed 65 tests with no failures/errors/skips. It included actual Spark SQL and Streamlit form tests, not mocked database aggregation. Public CSV-to-Parquet verification checked all 493,220 rows. Read-only migration checked every operational record after saving. Consult VERIFICATION.md for final additional runs and results.

## Remaining constraints

The dashboard refinement adds bounded query caching keyed by immutable file paths, modification times and sizes. A new snapshot invalidates cached reads. Public reports also cache by namespace/version/filter; **Refresh data** clears both caches. Live inventory queries include their current timestamp and do not reuse old current-stock values. New regressions cover cache invalidation and complete-period/missing-day analytical interpretation.

An extended 67-test run encountered two AppTest timeouts. After reducing repeated query work, the focused UI regressions and subsequent 75-test run passed. Final evidence and the larger final suite are recorded in VERIFICATION.md; the timeout run is not counted as a pass.

- Single-machine local disk; keep one normal app and one importer. Defensive multi-process tests do not establish distributed support.
- File locks are advisory to cooperating application writers; external edits can bypass them.
- Full operational snapshots are rewritten on mutation. This is suitable for the small demonstration, not high-volume OLTP.
- Old/unpublished versions remain intentionally. Disk growth and explicit recovery/retention are operational responsibilities; no automatic cleanup UI is provided.
- Correct host clock is necessary. There is no independent MongoDB server clock in this version.
- No multi-machine HDFS/YARN tests, clinical guarantees, authentication, compatibility decisions or power-loss certification.
- Source coverage is partial, latest year is incomplete, Rh and unit-level fields are absent; the 306 total discrepancy remains disclosed.
- Spark startup/first-query overhead is real. No claim that this migration makes interactive single-record operations faster.
- Verified Windows runs emit nonfatal missing-winutils/native-Hadoop warnings. Reads use Spark's Java fallback; writes use PyArrow. No unofficial executables were installed.
- PySpark 4.0.1's installed collection helper relies on garbage collection to close its socket stream; Python 3.14 unittest can report ResourceWarning. This upstream warning is disclosed, not hidden or represented as a failed business-rule assertion.
- Streamlit AppTest is an executable UI-path check, not a pixel-level browser screenshot review.

Preserved baseline: backups/mongodb-version-2026-10-29. Its old MongoDB results are historical, not evidence for Spark.
