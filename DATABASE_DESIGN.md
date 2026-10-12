# MongoDB database design

Database: `blood_bank_analytics`. Main collections: `donors`, `donations`, `blood_requests` only. BSON supports native Date, ObjectId, string, integer and null values. Dates in these examples use MongoDB Extended JSON: `$date` is converted to a BSON Date when imported correctly, not stored as an ordinary string. Examples are illustrative; the generator calculates dates dynamically.

Every listed field is written by the application and generator. `_id` is MongoDB's required unique key. Readable ID uniqueness is enforced by unique indexes. Business required fields, enum values, relationships, date order and numeric ranges are validated in Python; this simple project does not install MongoDB JSON Schema validators. Direct manual database writes can therefore bypass business validation. Use the supplied operations and generator.

## 1. donors

Purpose: store donor details once so repeated donation records can reference a stable donor identity.

| Field | BSON type | Required / rules |
|---|---|---|
| `_id` | ObjectId | Required; unique; automatically assigned for UI writes |
| `donor_id` | string | Required; unique; 3–40 uppercase letters/digits/underscore/hyphen |
| `name` | string | Required; 2–100 characters; synthetic |
| `age` | int | Required; 18–65 demonstration validation, not eligibility advice |
| `gender` | string | Required; Female, Male, Other, Prefer not to say |
| `blood_group` | string | Required; A+, A-, B+, B-, AB+, AB-, O+, O- |
| `city` | string | Required; 2–100 characters |
| `contact_number` | string | Required; 10 ASCII digits; synthetic; string preserves leading zeros |
| `registration_date` | Date | Required; no future dates |

```json
{
  "_id": {"$oid": "670000000000000000000001"},
  "donor_id": "DNR-000001",
  "name": "Synthetic Student One",
  "age": 25,
  "gender": "Other",
  "blood_group": "O+",
  "city": "Pune",
  "contact_number": "0000000001",
  "registration_date": {"$date": "2026-01-01T00:00:00Z"}
}
```

Indexes: `_id` (automatic unique), `donor_id` (unique), `(blood_group, city)` for blood-group filtering and possible city refinement. Case-insensitive contains-city search uses an escaped regular expression and is not guaranteed to benefit from this compound index, especially when searching city alone.

## 2. donations

Purpose: represent each physical demonstration unit and its lifetime. Counting these documents directly supplies inventory.

| Field | BSON type | Required / rules |
|---|---|---|
| `_id` | ObjectId | Required; unique |
| `donation_id` | string | Required; unique readable donation event ID |
| `unit_id` | string | Required; unique readable unit ID |
| `donor_id` | string | Required; references `donors.donor_id` |
| `blood_group` | string | Required; copied from referenced donor |
| `donation_date` | Date | Required; registration date <= donation date <= now |
| `expiration_date` | Date | Required; donation date plus configured shelf life |
| `unit_status` | string | Required; Available, Issued or Expired |
| `request_id` | string or null | Field present; null unless Issued, then references a valid request |
| `issued_at` | Date or null | Field present; null unless Issued; issue time < expiration time |

```json
{
  "_id": {"$oid": "670000000000000000000002"},
  "donation_id": "DON-000001",
  "unit_id": "UNT-000001",
  "donor_id": "DNR-000001",
  "blood_group": "O+",
  "donation_date": {"$date": "2026-10-01T00:00:00Z"},
  "expiration_date": {"$date": "2026-11-12T00:00:00Z"},
  "unit_status": "Issued",
  "request_id": "REQ-000001",
  "issued_at": {"$date": "2026-10-12T10:00:00Z"}
}
```

Before this unit was issued, its status was Available and both `request_id` and `issued_at` were null. Once issued, it remains historical Issued stock even after November 12. An unissued unit expires at `2026-11-12T00:00:00Z`.

Indexes in addition to `_id`:

| Index | Purpose |
|---|---|
| `donation_id` unique | Reject duplicate donation IDs |
| `unit_id` unique | Reject duplicate unit IDs |
| `donor_id` | Find a donor's donation history |
| `donation_date` | Filter donation date ranges before aggregating |
| `(blood_group, unit_status, expiration_date, unit_id)` named `inventory_fefo` | Exact-group Available queries, expiration cutoff and deterministic FEFO order |
| `(unit_status, expiration_date)` | Expiration maintenance and all-group available/near-expiry filtering |
| `request_id` | Count issued units linked to each request using `$lookup` |

No TTL index is used: deleting expired units would destroy analytics history. No inventory counter collection is needed.

## 3. blood_requests

Purpose: store requested quantities and hospital details, with a convenient status summary for inspection in Compass.

| Field | BSON type | Required / rules |
|---|---|---|
| `_id` | ObjectId | Required; unique |
| `request_id` | string | Required; unique readable ID |
| `hospital_name` | string | Required; 2–100 characters; synthetic |
| `blood_group` | string | Required; exact group being requested |
| `units_requested` | int | Required; 1–10,000 demonstration limit |
| `units_fulfilled` | int | Required; 0 through requested quantity; repaired from issued records |
| `request_date` | Date | Required; no future dates |
| `request_status` | string | Required; Pending, Partially Fulfilled or Fulfilled |

```json
{
  "_id": {"$oid": "670000000000000000000003"},
  "request_id": "REQ-000001",
  "hospital_name": "Synthetic Pune Hospital",
  "blood_group": "O+",
  "units_requested": 2,
  "units_fulfilled": 1,
  "request_date": {"$date": "2026-10-12T00:00:00Z"},
  "request_status": "Partially Fulfilled"
}
```

Indexes: `_id` (automatic unique), `request_id` (unique), `(blood_group, request_date)` for group/date demand analytics. Unfiltered date queries are not guaranteed to use the compound index because the first indexed field is blood group. Indexes cost storage and make writes more expensive; add further indexes only after inspecting actual query patterns.

## Relationships and consistency

```text
donors.donor_id       1 ───────── many donations.donor_id
blood_requests.request_id 1 ─── many donations.request_id (only issued units)
```

MongoDB does not enforce relational foreign keys. The application checks donor existence before recording donations and request existence before issuing units. Blood group is deliberately duplicated in a donation document to make analytics simple and preserve its recorded group. Donor blood-group editing and linked-record deletion are not exposed in this demo.

At normal operation completion, `units_fulfilled` equals the number of Issued donations referencing the request. Zero means Pending; a value below demand means Partially Fulfilled; equality means Fulfilled. A transient mismatch can exist after a crash between the unit update and request-summary update because standalone MongoDB has no cross-document transaction here. Request lists/KPIs derive counts with `$lookup`, and `reconcile_requests()` repairs stored fields at startup and after every attempt. A retry counts previously issued units before issuing any more.

The unit update uses `find_one_and_update()` with status, request link, group and expiration predicates and a sort by expiration date then unit ID. MongoDB atomically claims only one matching unit. The Python process lock also prevents same-process callers from overfulfilling a request. Independently running processes or direct external edits remain outside the supported single-user demonstration workflow. See [MongoDB atomicity documentation](https://www.mongodb.com/docs/manual/core/write-operations-atomicity/).

The atomic claim also compares expiration and donation dates with MongoDB's command-time `$$NOW`. Its update pipeline records the same server timestamp in `issued_at`. This prevents an application clock running behind the server from admitting already-expired units. It is a command-time check, not a clinical timing guarantee across arbitrary server stalls or failovers. Dashboard reporting still uses the application's UTC clock; keep client/server clocks synchronized. See [MongoDB update pipelines and NOW](https://www.mongodb.com/docs/manual/reference/method/db.collection.updateone/).
