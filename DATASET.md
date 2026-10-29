# Public dataset: source, design and methodology

## Provenance and license

**Daily Blood Donations by Blood Group & State**, National Blood Centre / Ministry of Health Malaysia, via [data.gov.my](https://data.gov.my/data-catalogue/blood_donations_state).

- [Official CSV download](https://storage.data.gov.my/healthcare/blood_donations_state.csv)
- [License: CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- [Included unchanged CSV](data/raw/blood_donations_state.csv)
- [Machine-readable provenance and checks](data/dataset_manifest.json)

Retain this attribution when sharing: “National Blood Centre / Ministry of Health Malaysia, via data.gov.my. Daily Blood Donations by Blood Group & State. CC BY 4.0. Raw CSV unchanged; dates/counts typed and provenance fields added for Spark/Parquet; charts derived by this project.” No endorsement is implied. This license covers the dataset, not automatically the project's code.

An official source was chosen instead of a Kaggle mirror because publisher, license, field definitions and coverage are directly documented. This is a pre-existing internet dataset, not Faker output. No identifying donor details are supplied.

## Snapshot downloaded 13 October 2026

| Property | Verified value |
|---|---:|
| CSV / Spark/Parquet documents | 493,220 |
| Date-state observations (one subtotal each) | 98,644 |
| States / distinct dates | 13 / 7,588 |
| Date range | 2006-01-01 to 2026-10-10 |
| Raw file bytes | 12,662,901 |
| Published `all` donation total | 9,896,044 |
| Sum of four ABO categories | 9,895,738 |
| Difference | 306 across 285 date-state observations |

SHA-256: `c74746936383d51c92aecdcf4fda07d26d49bd224cd6ac715214a22e47ea44cc`.

The 9.9 million total is a **sum of counts**, not 9.9 million documents, unique donors, or available units. The application processed 493,220 aggregate documents. These figures describe the included snapshot; the online source changes.

## Fields and storage

| Field | Meaning | Spark/Parquet type |
|---|---|---|
| `date` | Donation calendar date | UTC timestamp, UTC midnight |
| `state` | Collection state, not donor residence | string |
| `blood_type` | `a`, `b`, `ab`, `o`, or subtotal `all` | string |
| `donations` | Recorded donation count | nonnegative integer |
| `source_row` | One-based CSV data row, excluding header | integer |
| `source_sha256` | Hash of the imported CSV | string |

All fields are required by the importer. Storage: **`data/spark/public`**, logical Spark table `daily_donations`. This separate analytical namespace prevents aggregate observations being confused with individual units in the three operational Parquet tables in `data/spark/management`. No fictional donor/unit links are created.

No native unique indexes exist in Parquet. Source validation enforces the natural key and source-row identity. Spark SQL uses Parquet scans, projections and filter pushdown where applicable. Python validates data; direct external writes bypass it. Repeat import and --verify-only perform full content verification.

For an exact schema see DATABASE_DESIGN.md. The imported source fields are date/state/blood_type/donations plus source_row and source_sha256; no ObjectId is stored.

## Avoiding misleading analytics

Each date/state has five rows: four ABO categories and `all`. **Never sum all five together.** Overall trends use `blood_type: 'all'`; distribution charts use only `a,b,ab,o`. A group filter selects that category. `SUM(donations)` measures donations; `COUNT(*)` measures rows.

The snapshot passes missing-value, duplicate-key, category, date and nonnegative-integer checks. Published totals exceed the four-group breakdown by 306 across 285 observations. Raw values are preserved, and the dashboard displays the difference for each selection. The cause is not inferred and missing categories/people are not invented.

Validation rejects changed columns, malformed/future dates, unknown states/groups, incomplete five-category sets, duplicates and invalid counts. Future publisher schema changes require reviewing the allowlist. Midnight UTC is a calendar-date storage convention, not an observed donation time.

The publisher describes 22 main BBISv2 sites, covering about 80% of national donations. Perlis/Labuan are absent; Putrajaya is included under Kuala Lumpur, as are certain Selangor mobile drives. Consult the official catalogue for full notes. Collection state is not residence or a population-adjusted donation rate. The latest year is incomplete; do not compare it directly with full annual totals.

**Not supplied:** Rh, donor identities/ages/gender/city/contact, unique donors, individual units, requests, issues, expiry or available stock. The real dashboard shows trends, ABO distribution and collection-state activity. Stock alerts and transactions remain **separately labeled synthetic demonstrations**. No aggregate rows are expanded into invented real units. If your evaluator requires real individual inventory transactions, this source alone cannot satisfy that requirement; an appropriately licensed unit-level dataset would be necessary.

## Import, run and verify

With dependencies and Java 21 installed, from the project directory (Spark starts automatically):

```powershell
.\.venv\Scripts\python.exe public_data.py
.\.venv\Scripts\python.exe public_data.py --verify-only
.\.venv\Scripts\python.exe -m streamlit run app.py
```

Open <http://localhost:8501>. Dashboard and Analytics default to **Public dataset (real)**. Filter dates/state/ABO; Analytics shows raw records, six pipelines and an explain comparison. The bundled CSV works offline after dependencies are installed. Download occurs only if the default CSV is missing.

The importer writes a typed Parquet snapshot with Snappy compression and 100,000-row groups using PyArrow, then publishes one manifest pointer. --batch-size remains accepted for old command compatibility but does not control Parquet row groups. Reimport fully verifies the existing snapshot and adds no duplicates. Previous files are retained. See README.md for local locking, replacement, recovery and disk-space limitations.

For a new snapshot, retain the old CSV as a backup, save the official download under another filename, then:

```powershell
python public_data.py --csv data/raw/blood_donations_state_new.csv --replace
# Review the target, then type exactly:
# REPLACE public.daily_donations
python public_data.py --csv data/raw/blood_donations_state_new.csv --verify-only
```

Only the public manifest switches to the new Parquet snapshot; previous version files and operational records stay untouched. Keep the CSV and matching manifest together. The manifest describes the last imported file; verification uses the default filename unless `--csv` is provided.

For evaluation: show source/license and CSV; explain rows versus counts; filter real charts; disclose the discrepancy; inspect pipelines/explain; then explicitly switch to simulation for FEFO and stock alerts.
