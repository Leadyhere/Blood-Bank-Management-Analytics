# Blood Bank Management and Inventory Analytics Using MongoDB

A complete college mini project using Python, MongoDB, PyMongo, Streamlit, Pandas, Plotly, Faker and python-dotenv. All people, contacts and hospitals are synthetic. This is educational software and is not suitable for clinical decision-making.

Repository: [Leadyhere/Blood-Bank-Management-Analytics](https://github.com/Leadyhere/Blood-Bank-Management-Analytics). The robustness review, reproduced problems, fixes and remaining limitations are in [ROBUSTNESS_REVIEW.md](ROBUSTNESS_REVIEW.md).

## Phase 1 — Scope and architecture

The application registers donors, records one blood unit per donation, serves exact-group hospital requests, and analyzes stock, expiration and historical trends. The emphasis is MongoDB queries, aggregation, bulk insertion and indexing. It models one standardized red blood cell unit with a **demonstration** shelf life of 42 days. Actual component storage rules vary; this configuration is not a medical guideline.

```text
Streamlit (app.py)
    ├── operations.py → validated inserts, atomic issuing, request reconciliation
    └── analytics.py  → MongoDB aggregation → Pandas → Plotly charts
                ↓ PyMongo connection (database.py)
MongoDB: blood_bank_analytics
    ├── donors
    ├── donations       ← source of truth for stock and issued quantities
    └── blood_requests  ← demand and repairable fulfillment summaries

seed_data.py → Faker + historical FEFO simulation → batched insert_many()
config.py   → .env configuration shared by all modules
```

Run **one Streamlit process** against this database. An in-process lock serializes write operations from its sessions. Each unit claim atomically changes Available to Issued with a request link, after checking its expiration. Request reads count linked Issued units; stored counters are repaired at startup and after fulfillment. This handles an interrupted request-summary update without issuing the same unit again. Multiple independently running writers are not supported: atomic unit updates alone do not prevent two processes from overfulfilling one request. A production design would need multi-document transactions and stronger operational controls.

## Phase 2 — Collections and sample documents

Read [DATABASE_DESIGN.md](DATABASE_DESIGN.md) for field types, required fields, complete Extended JSON examples, relationships and indexes. The three collections contain BSON documents. MongoDB supplies `_id` ObjectIds for new UI records. Unique readable IDs support the interface; string references join records without exposing ObjectIds to students.

No separate inventory counter exists. A unit is available only when `unit_status == "Available"`, `request_id == null` and `expiration_date > now`. A unit expires at its expiration timestamp, not at the end of that date. UI dates become midnight UTC; all displayed/filter dates use UTC. Issued units remain Issued even after their former expiration date passes, preserving historical usage.

## Phase 3 — Files and dependencies

The supplied workspace directory `bda` is the project root; renaming it is optional.

```text
bda/
├── app.py                 # Five sidebar pages, forms, charts and tables
├── database.py            # MongoDB connection and indexes
├── operations.py          # Validation, donor/donation/request operations, FEFO
├── analytics.py           # Aggregation pipelines and explain comparison
├── config.py              # Settings and UTC clock
├── seed_data.py            # Synthetic dataset and batched insertion
├── test_project.py         # unittest and real MongoDB/Streamlit tests
├── test_robustness.py      # Failure, concurrency, validation and reset regressions
├── verify_project.py       # Read-only audit / isolated opt-in scale checks
├── requirements.txt
├── .env.example
├── .env                   # Local configuration, ignored by git
├── .gitignore
├── README.md
├── DATABASE_DESIGN.md
├── AGGREGATIONS.md
├── TESTING.md
├── REPORT.md
├── PRESENTATION_AND_VIVA.md
├── VERIFICATION.md         # Actual local results, with limitations
├── ROBUSTNESS_REVIEW.md     # Audit findings, fixes and residual risks
└── SOURCE_CODE.md          # All code files in labeled blocks
```

Python's standard library supplies datetime, threading, unittest, heapq and UUIDs; these need no separate packages. `requirements.txt` contains only the six specified third-party libraries (Plotly is the chosen chart library). No extra application framework or database is used.

## Phase 4 — Complete implementation

The executable files are in this directory. [SOURCE_CODE.md](SOURCE_CODE.md) also presents every source/configuration file in a separately labeled code block, in dependency order. Reading order: `config.py`, `database.py`, `operations.py`, `analytics.py`, `app.py`, then `seed_data.py` and `test_project.py`.

The pages are Dashboard, Donor Management, Blood Inventory, Blood Requests and Analytics. Tables use 100-row pagination so all matching records can be inspected without loading thousands of rows into a widget. CRUD is scoped to the requirements: insert/read donors and donations, insert/read requests, and update inventory/request statuses through fulfillment and expiration. Historical records are not deleted or arbitrarily edited through the UI.

The dashboard includes six KPIs and all six requested charts; Analytics adds monthly requested-unit trends, raw pipeline outputs, database statistics and explain results. Blood-group selection applies to KPIs and charts. An optional date range applies only to historical donation, request and donor-registration charts. Current stock, expiration alerts and all-time KPI totals never become misleading historical stock estimates. Pending requests includes both Pending and Partially Fulfilled. Expired-unit KPI excludes units already issued. Near-expiry units satisfy `now < expiration_date <= now + 7 days`.

## Phase 5 — Synthetic seeding

`--records N` means **N donation documents**, plus `ceil(N/2)` donors and `ceil(N/5)` requests. It does not mean N total documents.

| Mode | Command | Donors | Donations | Requests | Total |
|---|---|---:|---:|---:|---:|
| Quick test | `python seed_data.py --records 1000` | 500 | 1,000 | 200 | 1,700 |
| Normal demo | `python seed_data.py --records 5000` | 2,500 | 5,000 | 1,000 | 8,500 |
| Analytics | `python seed_data.py --records 10000` | 5,000 | 10,000 | 2,000 | 17,000 |
| Optional volume demo | `python seed_data.py --records 25000` | 12,500 | 25,000 | 5,000 | 42,500 |

Run one size on an empty database. To replace an existing dataset, stop Streamlit and explicitly confirm:

```powershell
python seed_data.py --records 10000 --seed 42 --batch-size 1000 --reset
# At the prompt, type exactly:
# RESET blood_bank_analytics
```

There is intentionally no automatic yes flag. The script shows the database and existing counts first. It drops only `donors`, `donations` and `blood_requests`, then recreates their indexes. Declining or entering different text cancels without deletion. Back up anything you want to retain first; collection deletion is not undoable. Without `--reset`, a nonempty database is refused.

Faker generates names prefixed with Synthetic. Contacts start with `000` and must not be used for contacting anyone. Cities come from an illustrative list; balanced donor groups are chosen for demonstration, not to represent population prevalence. Requests and donations span a year, with 35% of donations deliberately recent. Historical requests are replayed in date order with per-group FEFO queues, so issued units existed and were unexpired when issued. Up to 2% of all donations are reserved from already-old candidates as an unallocated waste sample, keeping expiration demonstrations visible even at 25,000 donations. These reserved units are never offered to historical requests. A shelf life longer than the dataset's one-year history can legitimately produce no expired units. Unissued old units become Expired. Counts, request links and blood groups remain consistent.

The default seed is 42. For the same seed, sizes, settings, Faker version and UTC calendar date, output is reproducible, including generated ObjectIds. The date is taken dynamically at runtime; the next day's generated timestamps shift. Faker upgrades may change names. The generator is an operational simulation: it does not enforce clinical donor eligibility or inter-donation intervals. A failed batch can leave a partial dataset; inspect it and use an explicitly confirmed reset before retrying. Do not run the generator concurrently with the app. Data is generated in memory, appropriate to these demonstration sizes.

`insert_many()` sends batches of up to 1,000 documents instead of making one network request per document. For 5,000 donations, this takes five donation batches, plus the donor and request batches. MongoDB still performs validation/index work per document; bulk insertion mainly reduces round trips and command overhead.

## Phase 6 — Windows setup and execution

### 1. Install Python and VS Code

Install a supported 64-bit Python (3.12 or newer) from [python.org](https://www.python.org/downloads/windows/), enabling PATH during installation. Install VS Code and its Python extension. Open a new PowerShell terminal and check:

```powershell
python --version
python -m pip --version
```

If `python` is not found but the Python launcher exists, use `py` for the initial virtual environment command. Select the project's `.venv\Scripts\python.exe` in VS Code using **Python: Select Interpreter**.

### 2. Install and start MongoDB Community Server

Use the [official Windows installation guide](https://www.mongodb.com/docs/v8.0/tutorial/install-mongodb-on-windows/) and the [Community Server download](https://www.mongodb.com/try/download/community). Choose the Windows MSI for a supported release, use the Complete installation, and install MongoDB as a Windows service. MongoDB 8.x is suitable; the queries use features available in MongoDB 5.0 and later. Compass is a GUI client, not the database server.

Check the service in PowerShell:

```powershell
Get-Service MongoDB
# If stopped, run the next command in an Administrator PowerShell:
Start-Service MongoDB
```

If installed without a service, create a dedicated data directory and run the installed binary in a separate terminal; adjust the version directory to match your installation:

```powershell
New-Item -ItemType Directory -Force C:\mongodb-data
& 'C:\Program Files\MongoDB\Server\8.2\bin\mongod.exe' --dbpath C:\mongodb-data --bind_ip 127.0.0.1 --port 27017
```

Use either the service or this foreground process, not both on port 27017. Leave the foreground terminal open during the demonstration. [MongoDB's installation documentation](https://www.mongodb.com/docs/v8.0/tutorial/install-mongodb-on-windows/) describes both approaches.

### 3. Open the project directory

For this supplied project:

```powershell
Set-Location C:\Users\avira\Desktop\bda
code .
```

If recreating on another computer, create a directory and copy the supplied files there:

```powershell
New-Item -ItemType Directory -Force C:\Projects\blood-bank-analytics
Set-Location C:\Projects\blood-bank-analytics
# Copy the supplied project files into this directory, then:
code .
```

### 4. Create and activate a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If script execution blocks activation, no policy change is necessary. Use the interpreter directly for all commands:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe database.py
```

### 5. Configure .env

Create `.env` from the example only if it is missing:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Default content:

```dotenv
MONGODB_URI=mongodb://localhost:27017/
MONGODB_DB=blood_bank_analytics
SHELF_LIFE_DAYS=42
LOW_STOCK_THRESHOLD=5
NEAR_EXPIRY_DAYS=7
```

Keep credentials in `.env`, which is ignored by git. Settings must be positive integers. Restart Streamlit after configuration changes. Changing shelf life affects new donations; it does not rewrite saved expiration dates.

Shelf life and near-expiry days accept 1–3,650; the stock threshold accepts 1–1,000,000. These are overflow/input guards, not clinical ranges. Each database operation has a 10-second driver timeout, with a five-second server-selection timeout. Connection/URI/database-name failures are sanitized, and `w=0` (unacknowledged writes) is rejected. Issuing uses MongoDB's command-time `$$NOW` as an additional expiry guard and as the issue timestamp, avoiding reliance on a lagging application clock.

### 6. Check MongoDB and seed data

```powershell
python database.py
python seed_data.py --records 5000
python database.py
```

On a newly seeded 5,000-donation database, the collection counts are 2,500 donors, 5,000 donations and 1,000 requests. If the supplied workspace is already seeded, skip the generator. It will safely refuse to append to existing data.

### 7. Launch the application

```powershell
python -m streamlit run app.py
```

Open **http://localhost:8501**. Keep the terminal running. Use Ctrl+C to stop it. Use the sidebar's Refresh data button after leaving the app idle or changing records outside it.

### 8. Inspect with MongoDB Compass

Install [MongoDB Compass](https://www.mongodb.com/try/download/compass), connect to `mongodb://localhost:27017/`, then open `blood_bank_analytics`. Inspect Documents and Indexes in all three collections. A filter such as `{"blood_group":"O+"}` shows matching records. Clear filters to inspect the full dataset. Compare an Issued unit's `request_id` with its request document. Do not manually change linked records during the demo.

If `mongosh` is installed separately, these commands are also useful:

```javascript
use blood_bank_analytics
db.donors.countDocuments({})
db.donations.countDocuments({})
db.blood_requests.countDocuments({})
db.donations.find({unit_status: "Available", request_id: null, expiration_date: {$gt: new Date()}}).limit(5)
db.donations.getIndexes()
```

### 9. Optional Atlas connection

The local server is the default. Atlas can be used by setting its connection URI in `.env`, creating a database user and permitting your client IP in Atlas's network access settings. Use the URI provided by Atlas's Connect dialog; do not commit its credentials. No code changes are required. See [Atlas connection instructions](https://www.mongodb.com/docs/atlas/connect-to-database-deployment/). Do not expose a local unauthenticated database to the internet.

### Troubleshooting

| Symptom | Check / action |
|---|---|
| Cannot connect to MongoDB | Check `Get-Service MongoDB`, port 27017 and `.env`; Compass alone does not run the server. |
| `mongod` is not recognized | Service mode needs no PATH entry; use the full binary path if running manually. |
| `mongosh` is not recognized | Install MongoDB Shell separately or use Compass/Python commands. |
| `ModuleNotFoundError` | Run pip and Streamlit using the same `.venv\Scripts\python.exe`. |
| Activation script blocked | Use the virtual environment interpreter directly as shown above. |
| ID already exists | Enter a different ID or leave blank for an automatically generated ID. |
| Donation before registration | Choose a date on or after this donor's registration. |
| No available units | Current stock excludes expired and issued units. Record a fresh donation with the requested group. |
| Fewer units issued than requested | Insufficient exact-group stock, or the per-click maximum is smaller than demand. Partial fulfillment is expected. |
| Empty historical charts | Clear date/group filters; donor registration dates can precede donation dates by many months. |
| Database not empty when seeding | Keep current data or stop the app and use `--reset`, reading the confirmation carefully. |
| Duplicate indexes / malformed external data | Use the app or generator to write data. For disposable synthetic data, use a confirmed reset. |
| Server interrupted during issuing | Restart/reconnect. Reads derive counts from linked units; startup or a retry repairs stored request summaries. |
| Port 8501 already in use | Stop your earlier app process before restarting; do not run independent writers on different ports. |
| Settings do not change | Restart Streamlit; dotenv does not override already-set process environment variables. |

## Phase 7 — Testing and evaluation demonstration

Read [TESTING.md](TESTING.md) for expected outcomes and a ten-minute demonstration sequence. Run:

```powershell
python -m unittest discover -v
# Include real MongoDB tests and Streamlit AppTest:
$env:RUN_MONGO_TESTS='1'
python -m unittest discover -v
Remove-Item Env:RUN_MONGO_TESTS
python analytics.py --explain --group 'O+'
python verify_project.py
# Optional: writes only to a temporary test database, then removes it:
python verify_project.py --scale
```

Integration tests create a random `blood_bank_test_<uuid>` database and remove only that temporary database afterward. The normal demo database is preserved. See [VERIFICATION.md](VERIFICATION.md) for observed results from this workspace; do not present expected outcomes as new measured results.

[AGGREGATIONS.md](AGGREGATIONS.md) explains every required MongoDB pipeline, index experiment and the 3 Vs. This is a small-scale simulation of big data techniques, not a distributed big data system. No million-record performance claim is made.

## Phase 8 — College materials

- [REPORT.md](REPORT.md): all 20 report sections, including a 150–200-word abstract.
- [PRESENTATION_AND_VIVA.md](PRESENTATION_AND_VIVA.md): ten-slide content with speaker notes and 20 short viva answers.
- [DATABASE_DESIGN.md](DATABASE_DESIGN.md): schemas, examples and relationships.
- [AGGREGATIONS.md](AGGREGATIONS.md): query walkthroughs and execution-statistics interpretation.

The presentation is supplied as editable content, ready to paste into slides. Screenshots should be captured from your actual run; no simulated screenshots or fabricated results are included.
