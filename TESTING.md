# Spark migration QA and demonstration

Run all tests from the project root using the same virtual environment as Streamlit.

~~~powershell
$env:RUN_SPARK_TESTS='1'
.\.venv\Scripts\python.exe -m unittest discover -v
Remove-Item Env:RUN_SPARK_TESTS
.\.venv\Scripts\python.exe public_data.py --verify-only
.\.venv\Scripts\python.exe verify_project.py
~~~

Without RUN_SPARK_TESTS, Spark/UI-dependent tests are skipped explicitly. Do not count skips as passes. Tests create and remove only randomly named temporary directories; live Parquet and old MongoDB are not deleted.

## Coverage

test_project.py: record validation, IDs, donors/references, chronology, exact-group FEFO, tie breaking, expiry boundaries, insufficient stock, request states, retries, same-process concurrency, failed publication, old-snapshot readers, deterministic seed, SQL totals, filters, literal search, pagination, thresholds and full Streamlit form workflow.

test_public_data.py: raw schema/type/category checks, duplicates, download behavior, source discrepancy, idempotency, full verification, guarded replacement, failed-write preservation, changed/missing rows, six real Spark queries and public-page filters/explain.

test_robustness.py: malformed/missing/path-escaping manifests, invalid IDs/contact/settings, uncertain save acknowledgement, independent-process writers, cancelled reset, expiration recheck and friendly UI errors.

test_presentation.py: correct selected denominators, complete-month comparisons, zero baselines, weekday normalization, seven consecutive calendar days for averages and explicit partial-period chart markers. Cache invalidation is also tested.

qa_visual.py runs an optional actual Chromium review of the real-data dashboard. It checks tab navigation, state filtering, CSV controls and a narrow viewport, then saves screenshots and qa_results/visual_review.json. It requires the development-only Playwright package; keep the Streamlit server running first.

The old MongoDB tests are preserved in backups/mongodb-version-2026-10-29, not claimed as Spark tests.

## Manual checklist and expected outcomes

| Action | Expected outcome |
|---|---|
| Run database.py | Spark 4.0.1 starts; Parquet row counts printed; no MongoDB connection |
| Import public data twice | Second import verifies and adds zero rows |
| Full source verification | 493,220 rows match the included CSV |
| Public full-snapshot dashboard | Total 9,896,044; ABO sum 9,895,738; disclosed difference 306 |
| Filter date, state, ABO | All public charts/KPIs follow selection |
| Open Analytics | SQL text, Parquet metadata and actual Spark physical plan |
| Register donor twice with same ID | Second insert rejected, count unchanged |
| Donate against unknown donor | Rejected without a published change |
| Add old donation | Expired, excluded from available stock |
| Request more than exact-group stock | Available units issued; request remains partial |
| Repeat completed request | Zero additional units issued |
| Inspect earliest expiry | FEFO chooses earliest; unit ID breaks ties |
| Expiration passes while app idle | Current query excludes it; management refresh updates label |
| Stock below 5 / zero stock | Warning includes group |
| Source or storage error | No silent fallback to fabricated or empty data |
| Restart application | Existing data persists through current.json |
| Stop old MongoDB service (optional manual check) | Spark app should still work; do not stop unrelated services for a test |

## Evaluation sequence

Show official dataset and license; show Parquet storage and schemas; run public dashboard; explain the "all" subtotal rule; inspect SQL/physical plan; switch explicitly to simulation; register → donate → request → issue; show partial stock and expiry; finish with actual test evidence and local-only limitations.

No test suite proves the absence of all possible defects. See VERIFICATION.md for observed results and ROBUSTNESS_REVIEW.md for remaining platform/design constraints.
