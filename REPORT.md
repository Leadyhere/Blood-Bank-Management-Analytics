# Blood Bank Management and Inventory Analytics Using MongoDB

## 1. Project title

**Blood Bank Management and Inventory Analytics Using MongoDB**

Subject: Big Data Analytics mini project. Add the student's name, roll number, institution, guide and academic year before submission.

## 2. Abstract

Blood banks need clear records of donors, donations, available units and hospital requests. This mini project develops a local application titled Blood Bank Management and Inventory Analytics Using MongoDB. Python implements the application logic, MongoDB stores document records, and PyMongo connects the application to the database. Streamlit provides an interactive interface, while Pandas and Plotly organize and visualize query results. Faker generates synthetic donors, donations and requests for safe classroom demonstrations. The system calculates inventory directly from donation records, excludes expired units, and fulfills exact blood group requests using the first expire, first out method. It displays donor and donation totals, current stock, outstanding requests, expiration alerts and historical trends. MongoDB aggregation pipelines summarize inventory, monthly donations, requested quantities, donor cities and unit statuses. Bulk insertion supports configurable demonstration datasets, and an explain comparison examines indexed and collection scan execution. The project illustrates volume, variety and velocity at a small scale without claiming distributed processing. It includes input validation, unique identifiers, tests and documented consistency limitations. The application is intended for learning and demonstration, not clinical decisions or operational blood bank deployment.

## 3. Introduction

Inventory information is useful only when it describes units that can actually be used within the model. A record marked Available may become expired as time passes, while a previously issued unit should remain in historical reports. Separating these meanings is central to this project.

MongoDB stores donor, donation and request information in document collections. Its aggregation framework performs descriptive analysis close to the stored data. Streamlit presents the results through a simple browser interface. The project combines record management with analytical reporting so students can explain both database operations and the meaning of the charts.

## 4. Problem statement

A classroom blood bank scenario requires a consistent way to register donors, record units, fulfill hospital demand and summarize trends. Manually maintained totals can disagree with individual unit records, and expired stock can be mistakenly included if only status labels are checked. The problem is to build a small local system that derives stock correctly, updates unit allocation safely, and presents useful analytical summaries using MongoDB.

## 5. Objectives

- Maintain synthetic donor, donation and request records with validated inputs and unique readable IDs.
- Calculate available inventory directly from unissued, unexpired donation documents.
- Fulfill exact-group requests using the earliest expiration date first.
- Show low stock, expired units and units approaching expiration.
- Analyze donation trends, demand, blood-group distribution and donor locations.
- Demonstrate aggregation pipelines, indexing, bulk insertion and query execution statistics.
- Provide a reproducible dataset, tests and a project that students can explain during evaluation.

## 6. Existing system and its limitations

For this educational comparison, the existing system is assumed to be a manually maintained register or spreadsheet. Such a workflow can require repeated counting, manual expiry checks and manual preparation of charts. Duplicate identifiers or separately maintained stock totals can introduce inconsistencies. Searching across donor, donation and request histories may be inconvenient.

This comparison is a project assumption. No survey of real hospitals was conducted, and the report does not claim that actual blood banks universally use manual methods.

## 7. Proposed system

The proposed system stores three types of documents in MongoDB. Donors are registered first; donation records reference them and copy their blood group. Each donation represents one standardized demonstration red blood cell unit. Requests describe exact-group demand. Issuing updates individual units and associates them with a request.

The application offers five pages: Dashboard, Donor Management, Blood Inventory, Blood Requests and Analytics. It calculates stock from records rather than maintaining a separate counter. Current expiration rules are applied to queries, and a refresh updates stored expired statuses. All data is synthetic.

## 8. Technology stack

| Technology | Responsibility |
|---|---|
| Python | Application logic, validation, seed simulation and tests |
| MongoDB Community Server | Local document storage, indexes and aggregation |
| PyMongo | Database queries, atomic updates and bulk insertion |
| Streamlit | Sidebar navigation, forms, tables and dashboard |
| Pandas | Format small aggregated result sets and fill internal monthly gaps |
| Plotly | Interactive bar, line and pie charts |
| Faker | Reproducible synthetic donor names |
| python-dotenv | Load connection/configuration settings from `.env` |

Python's standard library provides the test runner, time utilities, in-process lock and priority queues. No machine learning or distributed processing framework is used.

## 9. System architecture

The interface calls operations and analytics modules. Both use the PyMongo database connection. The operations module validates writes and implements fulfillment, while the analytics module constructs server-side pipelines. Configuration is read centrally. A separate script generates and inserts the demonstration data.

```text
User → Streamlit → operations.py / analytics.py → PyMongo → MongoDB
                          ↑                           ↑
                     config.py              seed_data.py (bulk load)
```

This separation allows tests to call database logic independently of the interface. MongoDB returns summarized values for most charts, limiting the amount of data transferred to Pandas.

## 10. Database design

The database is named `blood_bank_analytics` and contains three main collections. `donors` stores donor identity, demographic fields, group, city, synthetic contact and registration date. `donations` stores donation and unit IDs, donor reference, group, donation/expiration dates, status and any request association. `blood_requests` stores hospital demand, request date and fulfillment summary.

Each collection uses an ObjectId `_id` and a unique readable identifier. Donations also have a unique unit ID. The relationships are one donor to many donations and one request to many issued units. Python checks references; MongoDB does not provide foreign-key enforcement here. The donation's blood group is copied for convenient historical analysis.

Complete field definitions, required values, example Extended JSON documents and indexes are provided in `DATABASE_DESIGN.md`. Stored request counters are repairable summaries; linked issued units are authoritative. Inventory is always calculated directly.

## 11. Module descriptions

**Donor management:** validates donor details, creates readable IDs, rejects duplicates, and searches by group or city. Tables provide pagination over all matching records.

**Donation and inventory management:** requires an existing donor, calculates expiration using configured shelf life, and records one unit per donation. Available stock excludes elapsed expiration dates and issued records. Expired units remain available for historical analysis rather than being deleted.

**Request management:** records hospital demand, shows pending/partial/fulfilled status, and issues exact-group units using FEFO. The issue quantity is capped by outstanding demand, available stock and the operator's optional per-click limit.

**Analytics dashboard:** presents six KPIs, six core charts, filters, raw aggregation outputs and database statistics. A monthly requested-unit chart supplements the required visuals.

**Alerts:** identifies every group below the configured threshold, including groups with zero units, and lists near-expiry and expired unissued units. Alerts appear only inside the application.

## 12. Big Data Analytics techniques used

The project demonstrates descriptive analytics through MongoDB pipelines. Bulk insertion loads many documents per request. Useful indexes support selective filtering, donor references, request links and FEFO access. Explain statistics compare a forced collection scan with indexed access for the same inventory aggregation.

Volume is represented by configurable datasets, including 1,000, 5,000, 10,000 and 25,000 donations plus related records. Variety is represented by different document structures and BSON data types. Velocity is illustrated by interactive updates that change stock and subsequent analytics. These are small-scale demonstrations; the project is not a distributed big data platform and does not claim million-record processing.

## 13. MongoDB aggregation queries

The required pipelines are implemented in `analytics.py` and explained with executable examples in `AGGREGATIONS.md`:

| Pipeline | Main stages | Output |
|---|---|---|
| Available inventory | `$match`, `$group`, `$sum`, `$sort`, `$project` | Valid available units per group |
| Monthly donations | `$match`, `$dateToString`, `$group`, `$sum`, `$sort` | Donation count per UTC month |
| Requested quantities | `$match`, `$group`, `$sum`, `$sort`, `$project` | Units requested per group |
| Top donor cities | `$group`, `$sum`, `$sort`, `$project`, `$limit` | Up to ten cities by donor count |
| Expiration counts | `$match`, `$count` | Expired and near-expiry unissued counts |
| Inventory status | `$project`, `$switch`, `$group`, `$sum`, `$sort` | Effective Available/Issued/Expired counts |

The request view additionally uses `$lookup` to count issued units linked to each request. `$sum: 1` counts documents, whereas `$sum: '$units_requested'` adds demand quantities. This difference is important for interpreting the request chart correctly.

## 14. Implementation details

The default MongoDB URI is local and is read from `.env`. Connection failures produce a useful message without showing credentials. Unique indexes enforce identifier uniqueness. Dates are stored as UTC BSON dates; date-only input becomes midnight UTC. The default shelf life is 42 days, but actual blood-component rules vary and are outside this demonstration.

Unit issuing uses `find_one_and_update` with a predicate requiring Available status, no request link, the exact requested group and an expiration date after the current time. Sorting by expiration date and unit ID selects the FEFO unit deterministically. Status, request link and issue time change atomically in that document.

Standalone MongoDB does not provide a multi-document transaction in this workflow. A Python lock serializes writers within one application process. Each fulfillment begins by counting already-linked units and ends by repairing the request summary. If interrupted, request reads still derive actual counts and a retry/startup repairs stored counters. Independent writer processes are outside the supported model.

The generator creates donors before donations, simulates requests in historical date order, issues only existing unexpired matching units, and inserts collections in batches. The fixed seed is reproducible relative to the same date and package version. Reset requires typed confirmation and never occurs automatically on a nonempty database.

A robustness revision adds type checks, bounded database operation timeouts, sanitized configuration failures, server-time expiration checks and a read-only integrity auditor. A small reserved waste sample keeps expired records visible at the advertised dataset sizes. All-group charts include zero-stock groups; a selected-group chart displays only the selected group. Details and reproduced failure cases are in `ROBUSTNESS_REVIEW.md`.

## 15. Testing and results

Testing combines Python unit tests, real MongoDB integration tests and Streamlit's AppTest. Test cases check record validation, identifiers, references, date boundaries, FEFO, partial/complete fulfillment, exact matching, interrupted summary recovery, concurrent same-process callers and aggregated totals. All five pages are exercised with empty and generated data.

The actual local test record and dataset measurements are maintained in `VERIFICATION.md`, which should accompany this report. The manual checklist in `TESTING.md` describes expected results for a student's demonstration; it is not a fabricated test log. For a new machine or changed implementation, rerun the tests and record the new outputs.

The explain experiment reports execution time, documents examined, keys examined and raw plans. Its indexed and forced-scan paths use the same query. A small timing sample does not establish a universal speed improvement. No screenshots or performance figures should be inserted unless captured from an actual run.

## 16. Advantages

The application is small enough to study and run locally. Its inventory has a clear record-based source of truth. Database-side aggregation supports meaningful charts without transferring every source document to the UI. Synthetic data supports repeatable classroom demonstrations without handling real donor information. Indexes and explain output make database behavior observable, while tests document important rules.

## 17. Limitations

The application supports one running writer process and does not provide cross-document transactional atomicity. Direct external edits can bypass Python validation. Request summary fields can briefly be stale after an interruption, although reads derive their authoritative values and reconciliation repairs them.

The project does not implement authentication, clinical donor eligibility, donor spacing rules, blood-component compatibility, cold-chain tracking, infectious screening or regulated traceability. The age and shelf-life settings are demonstration assumptions. Generated distributions are illustrative. There is no distributed processing, external streaming ingestion or background notification system. Displayed information refreshes on interactions rather than continuously.

## 18. Future enhancements

Suitable future work includes MongoDB schema validators, controlled audit history, additional donor-history reports, downloadable analytical summaries and more extensive failure testing. A genuinely multi-user operational system would require transactions, access controls, validated domain requirements and professional review. These enhancements are outside the mini project's current scope; no predictive model is proposed.

## 19. Conclusion

This project provides a practical demonstration of MongoDB-based record management and descriptive inventory analytics. Its main contribution is the connection between individual unit records, correct available stock, request fulfillment and understandable charts. Bulk insertion, aggregation, indexing and measured query plans support the Big Data Analytics learning objectives at a manageable local scale. The documented tests and limitations help students explain both what the system demonstrates and where its guarantees end.

## 20. References

1. MongoDB, [Aggregation pipeline](https://www.mongodb.com/docs/manual/core/aggregation-pipeline/).
2. MongoDB, [Indexes](https://www.mongodb.com/docs/manual/indexes/).
3. MongoDB, [Atomicity and transactions](https://www.mongodb.com/docs/manual/core/write-operations-atomicity/).
4. MongoDB, [Explain results](https://www.mongodb.com/docs/manual/reference/explain-results/).
5. PyMongo, [Collection API](https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html).
6. Streamlit, [Documentation](https://docs.streamlit.io/).
7. Pandas, [User guide](https://pandas.pydata.org/docs/user_guide/).
8. Plotly, [Python documentation](https://plotly.com/python/).
9. Faker, [Documentation](https://faker.readthedocs.io/).
10. python-dotenv, [Documentation](https://bbc2.github.io/python-dotenv/).

Project documentation prepared on 12 October 2026. Use the linked official documentation for version-specific details.
