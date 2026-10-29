# Spark migration verification — 29 October 2026

## Final real-data dashboard release

The professional dashboard refinement passed **77 automated tests**, zero failures/errors/skips, in **132.129 seconds**, with RUN_SPARK_TESTS=1. The final suite includes actual Spark SQL, Parquet failure injection, UI forms, cache invalidation and nine analytical-presentation tests. The earlier intermediate 67-test timeout run was investigated; a focused UI rerun passed, followed by the complete 75-test and final 77-test runs.

The interface now has Overview, Trends & patterns, and Source & data tabs, a monthly trend, ABO mix, collection-state ranking, selection-based findings, seven-day daily averages, observed-weekday normalization, partial-period markers and filtered CSV exports. Bounded caches invalidate on new snapshot files; Refresh data clears the caches.

Actual browser screenshots are included at qa_results/dashboard.png and qa_results/patterns.png. qa_results/visual_review.json records the latest browser check; qa_results/full_dataset.json records full-source comparison and seven page/mode checks. The browser review tests the real total, tabs, source/download controls, state filtering and a narrow viewport. Browser automation explicitly waits for the new filtered total rather than assuming a rerun has finished when a spinner is absent.

During live development, an already-running server retained an older imported chart helper. A clean restart picked up the final source; clean-start AppTest checks used the correct helper. Restart the application after replacing its source files.

These results were executed locally after replacing the active MongoDB backend. Historical MongoDB evidence is preserved in backups/mongodb-version-2026-10-29/VERIFICATION.md and is not presented as Spark performance.

## Environment

Windows PowerShell; Python 3.14.7; PySpark 4.0.1; PyArrow 23.0.1; project-local Eclipse Temurin Java 21 at .runtime/java21. The previous Java 8 on PATH was insufficient; the application selects the project runtime without changing the system Java installation. The existing virtual environment and Streamlit/Pandas/Plotly dependencies were retained. pip check reported "No broken requirements found."

Spark SQL smoke query SELECT 1 succeeded and reported version 4.0.1. Importing app, analytics, operations, public_data and database in a fresh Python process reported that pymongo was NOT loaded. No MongoDB connection is required by active application code.

## Migration and full-source verification

- Original source/configuration/docs copied to backups/mongodb-version-2026-10-29 (no secret .env copied).
- MongoDB read-only migration preserved 2,500 donors, 5,000 donations and 1,000 requests; every migrated record matched after ObjectId-to-string conversion. Source database was not modified.
- Official CSV imported into Parquet: 493,220 rows; all rows verified against the CSV.
- Reimport: imported=0, already_present=true, rows=493220.
- Source hash unchanged: c74746936383d51c92aecdcf4fda07d26d49bd224cd6ac715214a22e47ea44cc.
- Actual Spark SQL totals: published 9,896,044; ABO 9,895,738; difference 306; 98,644 subtotal observations, 7,588 dates, 13 states.

## Complete automated suite

Executed:

~~~powershell
$env:RUN_SPARK_TESTS='1'
.\.venv\Scripts\python.exe -m unittest discover -v
~~~

**65 tests passed, zero failures, zero errors, zero skips**, in **275.908 seconds**. This is test-suite duration, not a performance benchmark. The preliminary 29 storage/business-rule tests also passed in 13.654 seconds.

The suite executes real Spark SQL, actual Parquet saves, same-process and independent-process contention, simulated before/after-publication failures, read-back verification and Streamlit AppTest forms/pages. It does not substitute mocked aggregation results for Spark.

## Full persisted-dataset UI audit

Executed python qa_full_dataset.py. Machine-readable evidence: [qa_results/full_dataset.json](qa_results/full_dataset.json).

Result: **PASS**, Spark 4.0.1, **76.890 seconds** including source comparison, Spark startup, operational audit, seven page/mode checks and explain.

Passed:
- Public Dashboard (displayed 9,896,044).
- Public Analytics.
- Simulated Dashboard.
- Donor Management.
- Blood Inventory.
- Blood Requests.
- Simulated Analytics.

The public pages used the real 493,220-row snapshot. The management pages used a temporary copy of the actual migrated 8,500 records, so UI expiry-refresh writes did not alter the user's saved operational snapshot.

The original management audit reported zero integrity errors. Effective statuses at this check: 1,286 available, 2,913 issued and 801 expired. 642 persisted Available labels had become stale as time passed; current-stock queries already excluded those expired units. Opening a management page normally refreshes those labels. These time-dependent figures are not permanent expectations.

Actual Spark physical plan and an observed elapsed query time are included in full_dataset.json. Do not reuse old MongoDB index timings or claim these observations establish Spark superiority.

## Scale checks

Executed python verify_project.py --scale in an isolated temporary storage directory. **All four sizes passed with zero audit errors and zero stale status flags.**

| Donation rows | Total operational rows | Available | Issued | Expired | Generate/save seconds | Audit seconds |
|---|---:|---:|---:|---:|---:|---:|
| 1,000 | 1,700 | 368 | 477 | 155 | 1.264 | 32.700 |
| 5,000 | 8,500 | 1,952 | 2,806 | 242 | 0.338 | 6.568 |
| 10,000 | 17,000 | 4,013 | 5,691 | 296 | 0.675 | 7.446 |
| 25,000 | 42,500 | 10,055 | 14,329 | 616 | 1.383 | 8.100 |

Audit includes independent record checks compared with Spark KPI/chart outputs. The first audit includes JVM startup. Scale and full-dataset QA overlapped on this machine; timings are observations, not controlled comparative benchmarks. Temporary test data was removed automatically and is reproducible from the generator; live data was not deleted.

## Reproduced bugs and fixes

- Windows read-only file flush raised OSError Bad file descriptor; writable flush handle fixed it.
- Import idempotency originally relied on stored hash/count; full content comparison now detects changed same-size data.
- Invalid configuration could raise before the Streamlit error guard; guarded imports fixed it.
- An uncertain save acknowledgement must not imply that nothing committed; error wording and retry tests now handle that case.
- MongoDB-specific updates, queries, ObjectIds, exceptions and index UI were replaced with validated snapshots, string IDs, Spark SQL and physical-plan output.

## Warnings and untested areas

Spark emits nonfatal missing-winutils/native-Hadoop warnings on this Windows setup; verified Parquet reads use its Java fallback and writes use PyArrow. No unofficial Hadoop binary was installed. Python 3.14 unittest reported ResourceWarning from PySpark 4.0.1's collection socket cleanup; the installed helper relies on garbage collection. Expected Streamlit bare-mode warnings occurred during AppTest. These were not assertion failures and are disclosed rather than hidden.

No HDFS/YARN cluster, multi-machine filesystem, power-loss test, clinical workflow or pixel-level browser screenshot audit was performed. There is no assertion that all conceivable defects have been found. Local clock correctness, snapshot retention/disk growth and external edits remain operational limitations; see ROBUSTNESS_REVIEW.md.
