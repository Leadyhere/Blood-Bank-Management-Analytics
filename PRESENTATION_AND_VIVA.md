# Ten-slide presentation content

This is editable slide content and speaker notes, not a rendered PowerPoint file. Add your institution details and screenshots from your actual application run.

## Slide 1 — Title

**Blood Bank Management and Inventory Analytics Using MongoDB**

Big Data Analytics Mini Project

Student name · Roll number · Department · Guide · Academic year

Speaker note: Introduce the project as a local educational application using synthetic data.

## Slide 2 — Problem and objectives

- Need a clear view of donors, donated units and hospital demand.
- Separate usable stock from issued and expired units.
- Identify low stock and approaching expiry.
- Analyze donation and request patterns using MongoDB.

Speaker note: The manual-record comparison is an educational assumption, not a survey of real hospitals.

## Slide 3 — Technology and architecture

**Streamlit → Python operations/analytics → PyMongo → MongoDB**

- Pandas formats query output; Plotly draws charts.
- Faker creates synthetic data.
- python-dotenv loads local configuration.
- One local MongoDB server and one app process.

Speaker note: Explain why separate UI, database operations and analytics files make the project easier to understand and test.

## Slide 4 — Database design

- `donors`: identity, group, city and registration details.
- `donations`: one document per unit, dates, status and request link.
- `blood_requests`: demand and fulfillment summary.
- ObjectIds identify documents; unique readable IDs support the interface.
- One donor → many donations; one request → many issued units.

Speaker note: Show a real Compass document. Stock is calculated from donation records, with no separate inventory counter.

## Slide 5 — Main application modules

- Dashboard: six KPIs, charts and alerts.
- Donor Management: register and search synthetic donors.
- Blood Inventory: record donations and inspect expiration.
- Blood Requests: add requests and fulfill demand.
- Analytics: inspect pipelines, statistics and explain results.

Speaker note: Show the sidebar and a populated dashboard screenshot captured during your run.

## Slide 6 — Correct inventory and FEFO

- Available means unissued and expiration time is still in the future.
- Default demonstration shelf life: 42 days.
- Exact requested blood group only.
- FEFO selects earliest expiration first.
- Atomic unit update prevents duplicate allocation.
- Linked issued records determine fulfilled quantities.

Speaker note: Demonstrate a partial request, then explain the single-process lock and the absence of a cross-document transaction. Actual storage rules vary; there is no clinical compatibility logic.

## Slide 7 — Analytics and alerts

- Inventory by group and donated-group distribution.
- Monthly donations and monthly requested units.
- Demand by group, inventory status and top donor cities.
- Low-stock threshold: below 5 units.
- Near-expiry window: next 7 days.
- Historical filters do not change current-stock alerts.

Speaker note: Explain that request demand sums units, not request documents, and that all eight blood groups appear even when stock is zero.

## Slide 8 — Big Data Analytics techniques

- `$match`, `$group`, `$sum`, `$sort`, `$project`, `$count`, `$dateToString`, `$limit`.
- `insert_many()` batches thousands of synthetic documents.
- Indexes support selective inventory and date queries.
- Explain compares a forced scan with indexed access.
- Volume, variety and velocity demonstrated at classroom scale.

Speaker note: Use the measured results in `VERIFICATION.md` or rerun explain live. Discuss examined documents before drawing conclusions from milliseconds.

## Slide 9 — Testing and limitations

- Validation, duplicate IDs, references and expiration boundaries.
- FEFO, insufficient stock, partial/complete fulfillment and retries.
- Aggregation totals and all five Streamlit pages.
- Synthetic data; one writer process; no clinical decisions.
- No claim of distributed or million-record performance.

Speaker note: Display actual test output. Clearly distinguish executed automated checks from manual cases still to be demonstrated.

## Slide 10 — Conclusion and future work

- MongoDB documents support a clear unit-based inventory model.
- Aggregation transforms stored records into useful descriptive reports.
- Bulk operations and indexes demonstrate database efficiency techniques.
- Possible additions: schema validators, audit history and analytical exports.
- Questions and live demonstration.

Speaker note: End by opening the app, rather than claiming unmeasured operational benefits.

# Twenty viva questions and short answers

1. **Why did you choose MongoDB?**

   It stores related information as documents and provides aggregation, indexes and bulk insertion. These features fit the subject's learning objectives and our three record types.

2. **What is Big Data Analytics?**

   It is the process of analyzing large, diverse or rapidly arriving data to find useful patterns and summaries. This project demonstrates some techniques on a small local dataset.

3. **Why is MongoDB called a NoSQL database?**

   It uses a document model rather than the traditional relational table model. NoSQL commonly means “not only SQL”; it does not mean the database has no query language.

4. **What is a MongoDB document?**

   A document is a record of field-value pairs stored in BSON. It can contain strings, numbers, dates, arrays and nested objects.

5. **What is a collection?**

   A collection is a group of MongoDB documents. Our collections are donors, donations and blood_requests.

6. **What is an ObjectId, and why also use readable IDs?**

   ObjectId is the default MongoDB document identifier. Readable IDs such as DNR-000001 are easier to search and explain in the interface. Unique indexes enforce their uniqueness.

7. **What is an aggregation pipeline?**

   It is a sequence of stages that filters, transforms and summarizes documents. Each stage passes its results to the next stage.

8. **What is the use of `$group` and `$sum`?**

   `$group` combines records sharing a key, such as blood group. `$sum: 1` counts records, while `$sum: '$units_requested'` adds the requested quantities.

9. **Why are indexes important?**

   They can help MongoDB find relevant documents without scanning the entire collection. They also use storage and add write overhead, so we choose indexes for actual queries.

10. **How do you identify low-stock groups?**

    Aggregate current available units by group, fill absent groups with zero, and compare each count with the configured threshold, which defaults to five.

11. **How are expired units handled?**

    An unissued unit is expired when its expiration timestamp is at or before now. Queries exclude it from available stock immediately, and the app refresh updates its stored status. It remains in history.

12. **How does FEFO work?**

    FEFO means First Expire, First Out. We sort eligible exact-group units by expiration date, then unit ID, and atomically issue the first one.

13. **How do you prevent the same unit from being issued twice?**

    `find_one_and_update` checks Available status and future expiration while atomically changing status and adding the request link. A later attempt cannot match the already-issued unit.

14. **How do request quantities remain consistent?**

    We count issued donation records linked to the request. Reads derive fulfillment from these records, and reconciliation updates stored summary fields. An in-process lock serializes fulfillment calls; separate writer processes are not supported.

15. **What is the difference between MongoDB and MySQL?**

    MongoDB primarily uses BSON documents and collections. MySQL uses relational tables, SQL and declared relationships/constraints. Both support indexes and useful data analysis; the choice depends on the application and learning goals.

16. **How does this project demonstrate Big Data Analytics?**

    It generates thousands of documents, loads them in batches, summarizes them with database-side pipelines, uses indexes and inspects execution plans. It demonstrates techniques rather than distributed scale.

17. **What are the 3 Vs in this project?**

    Volume is the configurable record count. Variety is the different donor, donation and request fields and types. Velocity is the arrival of new records and subsequent refreshed analytics; there is no high-rate streaming system.

18. **Why use `insert_many()` instead of repeated `insert_one()`?**

    Batching reduces the number of commands and network round trips. MongoDB still stores and indexes each document, but the client sends many records together.

19. **What does `explain()` tell you?**

    It shows the execution plan and, with executionStats, measured time, examined documents and examined index keys. We compare a forced collection scan with an indexed query without inventing benchmark values.

20. **What are the main limitations?**

    The app uses synthetic data, one writer process and no multi-document transaction. It has no clinical eligibility, compatibility, access control or regulated workflow. Its shelf life and age range are demonstration assumptions, and it is not a clinical system.
