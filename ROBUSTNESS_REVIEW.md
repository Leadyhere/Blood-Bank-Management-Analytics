# Robustness review — 12 October 2026

## Assessment

The project is suitable for a college demonstration using one local application process and synthetic data. The original 17 tests passed on re-run, but targeted negative tests exposed additional defects. These were fixed and covered by regression tests. No claim is made that testing proves the absence of all possible bugs.

## Problems reproduced and corrected

| Finding | Impact before correction | Correction / evidence |
|---|---|---|
| Client-clock-only expiration check | A clock ten seconds behind MongoDB allowed a unit expired one second earlier to issue in a real-database regression | Atomic claim now checks MongoDB command-time `$$NOW` and records server issue time; regression passes |
| Loose text and ID validation | `None`, numbers and collections could become donor text or trigger `AttributeError`; whitespace-only automatic IDs were rejected | Explicit string/control-character checks and readable-ID normalization; tests cover invalid types |
| MongoClient constructor and DB-name errors outside handling | Configuration exceptions could escape the connection wrapper without sanitization/cleanup | Constructor, ping, DB selection and write concern are checked together; errors omit URI details and close any opened client |
| No bounded wait for a stalled operation | Connect timeout alone did not bound later socket/operation waits | Added 10-second socket/operation timeout; kept five-second server selection timeout |
| Unacknowledged writes permitted by URI | `w=0` is incompatible with the app's acknowledged-write consistency assumptions | Such connections are rejected explicitly |
| Partial/invalid history-filter arguments | A single date boundary was silently ignored; unsupported groups could return misleading empty output | Both dates and group are validated; dates normalize to inclusive UTC calendar days |
| Excluded blood groups displayed as zero | A selected-group chart could imply that the other seven groups had no stock | Selected-group output now contains only that group; All still shows all eight |
| Large shelf life broke seed chronology | A 1,000-day setting generated some donations before donor registration | Recent-donation range is capped to the simulated year's history; chronology regression passes |
| No expired units at 25,000 donations | High utilization could remove the expired-stock demonstration entirely | Reserve up to 2% from old candidates as unallocated waste; all three statuses verified at each advertised size under default settings |
| Seed size / batch / pagination validation gaps | Invalid types could raise raw errors; a zero MongoDB cursor limit could mean unlimited rows | Type/range validation now rejects invalid sizes and unbounded page limits |
| Extreme numeric settings | Very large day counts could overflow date calculations | Bounded positive settings with clear errors and a friendly app configuration message |
| Redundant inventory-page calculations | Displaying stock ran every dashboard query, including all request lookups | Inventory page now performs only the required inventory aggregation |
| Cached connection setup could leak a client | Index/reconciliation setup errors after connection did not close the client | Failed initialization closes the opened client before propagating the sanitized UI error |

The original chart-ID collision had already been corrected during initial implementation and remains covered by the full form-flow test.

## Additional checks that passed

- Existing donor/donation/request validation and unique indexes.
- Exact blood-group matching, FEFO order and deterministic ID tie-breaking.
- No overissue after repeated fulfillment, partial fulfillment or insufficient inventory.
- Lost acknowledgement after a successful unit write: a retry sees the linked unit and does not issue another.
- Ten simultaneous same-process requests competing for seven units: seven distinct units issued; no duplicates or request overfulfillment.
- Startup/request-summary recovery, expired/near-expiry boundary calculations and issued-history preservation.
- Empty and populated pages, forms through actual issuance, group/date filters and explain UI.
- Existing-data seeding refusal, reset cancellation, and confirmed reset preserving an unrelated sentinel collection in a temporary fixture.
- Read-only auditor detects malformed references and wrong request summaries without changing them.
- Dependency consistency check (`pip check`) reported no broken requirements.
- Real database audits at 1,000, 5,000, 10,000 and 25,000 donations, plus related records.

Exact final counts, timings and observed outputs are in [VERIFICATION.md](VERIFICATION.md).

## Remaining limitations and their practical effect

1. **One writer process is a requirement.** `RLock` coordinates sessions in one Python process, not independently launched processes or computers. Multiple processes could overfulfill a request even though each unit is claimed atomically. Multi-process writes are unsupported; this review does not convert the demo into a production system.
2. **No cross-document transaction.** An interruption can leave stored request summaries stale until reconciliation. Reads derive linked-unit totals. A multi-unit fulfillment is incremental, not all-or-nothing.
3. **Direct edits bypass Python validation.** No database schema validators or foreign-key constraints are installed. Use the app/generator; run `python verify_project.py` to find malformed external changes. Detection is not automatic repair of arbitrary corruption.
4. **Clock and refresh assumptions.** Issuance checks server command time; dashboard calculations use client UTC. Synchronize clocks. Reports update on interaction, and read-only audits are not snapshot-isolated from simultaneous writes.
5. **Failure simulation is bounded.** Lost acknowledgements and interruptions were injected in tests. The shared MongoDB service was not deliberately killed, and hardware/power failure, replica failover and disk corruption were not tested.
6. **No clinical or public deployment guarantees.** No authentication, clinical compatibility, donor eligibility, regulated audit trail or operational blood-product handling is implemented. Synthetic contacts and the 42-day shelf life are demonstration inputs.
7. **Performance remains local.** The largest measured dataset contains 42,500 total documents (25,000 donations). Generation/audit use memory proportional to data size. Driver timeouts can stop unusually slow queries. No million-record or distributed benchmark is claimed.
8. **Visual verification is limited.** Streamlit AppTest validates page execution and form interactions. A pixel-level browser review was unavailable because the connected browser tool failed to initialize during initial verification. No screenshots are fabricated.

## Reproduce the review

```powershell
$env:RUN_MONGO_TESTS='1'
python -m unittest discover -v
Remove-Item Env:RUN_MONGO_TESTS
python verify_project.py
python verify_project.py --scale
python -m pip check
```

Tests/scale runs delete only their own random `blood_bank_test_<uuid>` databases. They preserve `blood_bank_analytics`; disposable test fixtures can be regenerated. The normal seed reset remains gated by exact typed confirmation.

## Publishing scope

The GitHub update includes source, tests, configuration example, setup guide, database/aggregation explanations, audit findings, actual verification observations, report, presentation content and viva answers. It excludes `.env`, credentials, the virtual environment, caches and the local MongoDB dataset. New users generate their own synthetic dataset with `seed_data.py`.
