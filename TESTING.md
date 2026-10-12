# Testing and demonstration guide

Use synthetic records only. Keep the normal app as the only writer while manually demonstrating. For automated tests, a separately named test database is used.

## Automated commands

```powershell
python -m unittest discover -v
```

This discovers both `test_project.py` and `test_robustness.py`. Without `RUN_MONGO_TESTS=1`, database-dependent tests are explicitly skipped; do not report the skipped cases as passed. Pure tests check validation, settings, connection-error sanitization, group/date filters, deterministic generation, shelf-life edge cases and generated relationship/status integrity.

```powershell
$env:RUN_MONGO_TESTS='1'
python -m unittest discover -v
Remove-Item Env:RUN_MONGO_TESTS
```

With MongoDB running, the suite also executes real database and Streamlit tests. It creates a random `blood_bank_test_<uuid>` database, inserts only its own fixtures, and drops that exact test database afterward. If a test process is killed before cleanup, an orphan test database may remain; inspect its name and contents before removing it. The demonstration database is never a cleanup target.

Tests cover duplicate IDs, missing donors, donor-group matching, FEFO selection, partial fulfillment, expired units with deliberately stale status, exact-group matching, repeat fulfillment, expiry boundaries, interrupted summary recovery, two same-process callers, threshold boundaries, aggregation totals, explain statistics, and all five pages with empty and seeded databases. The interruption fixture deliberately raises after the atomic unit update, then verifies a retry does not overissue. This does not simulate every network/server failure mode.

The robustness suite adds a lagging client clock, lost write acknowledgement, ten competing requests, equal-expiry FEFO ties, pagination, invalid types, extreme settings, unacknowledged-write rejection, nonempty seed refusal, cancelled/confirmed reset scope, and deliberate external-data corruption. All destructive cases operate only on a random test database. The confirmed-reset test uses an extra sentinel collection **only in the test fixture** to prove the three application collections are the only reset targets.

Run the read-only full-record audit while the app is idle:

```powershell
python verify_project.py
```

Run all four advertised dataset sizes in an automatically cleaned temporary database:

```powershell
python verify_project.py --scale
# Or choose a smaller subset:
python verify_project.py --scale --sizes 1000 5000
```

The auditor checks required values, reference links, quantities, timestamps, unique indexes and independent aggregation totals. It reports malformed records and stale request summaries; it never repairs them. It permits stale Available flags after expiration because stock queries correctly exclude those units. The scale check measures generation, insertion, audit and dashboard-function time separately. These timings are observations, not service-level guarantees or browser page-load timings.

## Manual checklist with expected outcomes

Record actual result, date and evidence during your own run. The outcomes below are expectations, not claims that every manual action has already been performed.

| Test | Action | Expected result |
|---|---|---|
| Connection | Run `python database.py` | Connected message, collection counts and no traceback |
| Empty database | Use a fresh configured demonstration database before seeding | Zero KPIs, eight low-stock groups, useful no-data messages |
| Add donor | Register `DNR-VIVA`, Synthetic Viva Student, age 25, O+, Pune, `0000000001`, today's registration | Success message; donor searchable by group/city |
| Duplicate donor | Register the same ID again | Friendly error; total count unchanged |
| Validation | Submit blank name or a 3-digit contact | Error; no donor inserted |
| Donation | Record `UNT-VIVA-1` for `DNR-VIVA` dated today | Blood group copied from donor; Available stock for O+ increases by one |
| Unknown donor | Record a donation using `DNR-NOT-FOUND` | Error; no new unit |
| Duplicate unit | Record `UNT-VIVA-1` a second time | Error; stock unchanged |
| Expiration | Register a separate donor with a registration date at least 60 days ago; record donation 50 days ago using default 42-day shelf life | Unit saved as Expired; available stock does not rise |
| Request | Add `REQ-VIVA-1`, Synthetic Demo Hospital, O+, two units, today | Pending, fulfilled zero |
| Partial issue | Choose issue maximum one for `REQ-VIVA-1` | One matching unit issued if available; status Partially Fulfilled |
| Complete issue | Issue again with sufficient O+ stock | Total fulfilled equals two; status Fulfilled |
| Repeat issue | Fulfill the completed request again | Zero additional units issued |
| FEFO | Inspect two same-group unexpired units with different expiry dates; issue one | Earlier-expiring unit chosen, even if entered later |
| Insufficient stock | Request more units than exact-group availability | Only available units issue; remaining demand stays open; stock stays at zero or above |
| Exact match | Have O- stock but request O+ with no O+ stock | No O- substitution occurs |
| Expired rejection | Attempt to satisfy demand when only expired units remain | Zero issued; expired records retain no request link |
| Low stock | Reduce a group's available stock below 5 | Warning includes that group; exactly 5 produces no warning |
| Near expiry | Record a donation 40 days ago under a donor registered earlier | Unissued unit appears in the next-seven-days list with default shelf life |
| Analytics totals | Compare Compass counts and aggregation sums with KPIs | Donor/donation counts match; effective status counts sum to total donations |
| Filter meaning | Change historical range | Trends change; current inventory KPIs and alerts do not change from the date range alone |
| Seeded data | Generate 5,000 donations on an empty database | 8,500 total documents and all six charts supported by actual records |
| Pagination | Browse successive donor or inventory pages | Each page shows up to 100 records; all matches are accessible |
| Safe reset cancellation | Run generator with `--reset` and type `cancel` | No collection is dropped; counts unchanged |
| Query plan | Run `python analytics.py --explain` | Real statistics for collection-scan and indexed paths; no guaranteed timing ratio |

The seeded database already contains stock and requests. To make a manual FEFO or insufficient-stock example predictable, inspect current stock first or use a fresh separate demo database. Do not assume the synthetic unit just entered is the earliest-expiring unit in a populated group.

## Ten-minute evaluation sequence

1. **Minute 0–1:** State the problem and show the simple architecture: Streamlit → Python → PyMongo → MongoDB.
2. **Minute 1–2:** In Compass, show three documents and unique/read-optimized indexes. Explain one donor to many units and one request to many issued units.
3. **Minute 2–4:** Open Dashboard; explain the six KPIs, inventory bar chart, monthly donations, demand and expiry alerts. Show historical filter behavior.
4. **Minute 4–5:** Register a synthetic donor and record one donation. Show its group inherited from the donor and updated stock.
5. **Minute 5–6:** Add a two-unit request, issue at most one, then complete it if stock permits. Show partial status, FEFO and inventory changes.
6. **Minute 6–7:** Inspect an expired unit and explain why the issue filter excludes it even if a status flag were stale.
7. **Minute 7–8:** Open Analytics; expand monthly donation and requested-unit pipelines. Explain `$match`, `$group` and `$sum`.
8. **Minute 8–9:** Run explain comparison. Discuss documents examined and why timing alone is noisy.
9. **Minute 9–10:** Show test results and limitations: single writer process, synthetic data, no clinical eligibility logic, no distributed-scale claim.

Suggested screenshots for your report: populated dashboard, donor form with a success message, inventory/expiration list, partially fulfilled request, a pipeline with output, Compass indexes, actual test summary and actual explain output. Capture them yourself from the running project rather than substituting illustrative images.
