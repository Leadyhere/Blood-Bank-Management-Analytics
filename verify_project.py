"""Read-only integrity audit, plus an opt-in isolated scale demonstration.

python verify_project.py                 # audit configured database, no writes
python verify_project.py --scale         # create/drop only a random test DB
"""
import argparse
import json
from collections import Counter
from datetime import datetime
from time import perf_counter
from uuid import uuid4


import analytics
import operations as ops
from config import BLOOD_GROUPS, GENDERS, NEAR_EXPIRY_DAYS, utcnow
from database import connect, Store, StorageError
from tempfile import TemporaryDirectory
from seed_data import generate_dataset, insert_dataset


def audit_database(db):
    """Inspect all records and compare independent counts with aggregations.

    Run while the single-user demo is idle: this is not a transaction snapshot.
    Invalid manual edits are reported, never silently repaired.
    """
    now = utcnow()
    errors = []
    total_errors = 0
    def problem(message):
        nonlocal total_errors
        total_errors += 1
        if len(errors) < 50:
            errors.append(message)
    def require(condition, message):
        if not condition:
            raise ValueError(message)
    def date_value(row, key):
        require(isinstance(row[key], datetime), f'{key} is not a timestamp')
        return ops.as_utc(row[key])
    def identifier(row, key, prefix):
        require(isinstance(row['_id'], str) and bool(row['_id']), '_id is not a string identifier')
        require(ops.readable_id(row[key], prefix, generate=False) == row[key], f'invalid {key}')
    snapshot = db.manifest()
    donors = db.rows('donors', snapshot)
    requests = db.rows('blood_requests', snapshot)
    donor_map = {row.get('donor_id'): row for row in donors if isinstance(row.get('donor_id'), str)}
    request_map = {row.get('request_id'): row for row in requests if isinstance(row.get('request_id'), str)}
    for row in donors:
        try:
            identifier(row, 'donor_id', 'DNR')
            ops.text_field(row['name'], 'Name')
            ops.text_field(row['city'], 'City')
            ops.integer(row['age'], 'Age', 18, 65)
            ops.blood_group(row['blood_group'])
            require(row['gender'] in GENDERS, 'invalid gender')
            contact = row['contact_number']
            require(isinstance(contact, str) and len(contact) == 10 and contact.isascii() and contact.isdigit(),
                    'invalid contact number')
            require(date_value(row, 'registration_date') <= now, 'future registration')
        except (ValueError, KeyError, TypeError) as exc:
            problem(f"donors/{row.get('_id')}: {exc}")
    for row in requests:
        try:
            identifier(row, 'request_id', 'REQ')
            ops.text_field(row['hospital_name'], 'Hospital')
            ops.blood_group(row['blood_group'])
            ops.integer(row['units_requested'], 'Demand', 1, 10000)
            ops.integer(row['units_fulfilled'], 'Fulfilled', 0, row['units_requested'])
            require(date_value(row, 'request_date') <= now, 'future request')
            require(row['request_status'] in ('Pending', 'Partially Fulfilled', 'Fulfilled'), 'invalid request status')
        except (ValueError, KeyError, TypeError) as exc:
            problem(f"blood_requests/{row.get('_id')}: {exc}")
    issued_counts, stock, statuses = Counter(), Counter(), Counter()
    donation_months, donation_groups = Counter(), Counter()
    near_expiry = 0
    unit_ids, donation_ids = set(), set()
    donations = stale_statuses = 0
    for row in db.rows('donations', snapshot):
        donations += 1
        try:
            identifier(row, 'unit_id', 'UNT')
            identifier(row, 'donation_id', 'DON')
            require(row['unit_id'] not in unit_ids and row['donation_id'] not in donation_ids, 'duplicate unit/donation ID')
            unit_ids.add(row['unit_id'])
            donation_ids.add(row['donation_id'])
            donor = donor_map.get(row['donor_id'])
            require(donor is not None, 'missing donor reference')
            require(row['blood_group'] == donor['blood_group'], 'donor blood-group mismatch')
            donated, expires = date_value(row, 'donation_date'), date_value(row, 'expiration_date')
            require(date_value(donor, 'registration_date') <= donated <= now, 'donation chronology invalid')
            require(expires > donated, 'expiration does not follow donation')
            status = row['unit_status']
            require(status in ('Available', 'Issued', 'Expired'), 'invalid unit status')
            if status == 'Issued':
                request = request_map.get(row['request_id'])
                require(request is not None, 'missing request reference')
                require(request['blood_group'] == row['blood_group'], 'request blood-group mismatch')
                issued_at = date_value(row, 'issued_at')
                require(max(donated, date_value(request, 'request_date')) <= issued_at < expires,
                        'issue outside valid date interval')
                require(issued_at <= now, 'future issue timestamp')
                issued_counts[row['request_id']] += 1
                statuses['Issued'] += 1
            else:
                require(row['request_id'] is None and row['issued_at'] is None, 'unissued unit has issue metadata')
                if expires <= now:
                    statuses['Expired'] += 1
                    stale_statuses += status == 'Available'
                else:
                    require(status == 'Available', 'Expired flag before expiration')
                    stock[row['blood_group']] += 1
                    statuses['Available'] += 1
                    near_expiry += (expires - now).total_seconds() <= NEAR_EXPIRY_DAYS * 86400
            donation_months[donated.strftime('%Y-%m')] += 1
            donation_groups[row['blood_group']] += 1
        except (ValueError, KeyError, TypeError) as exc:
            problem(f"donations/{row.get('_id')}: {exc}")
    for row in requests:
        request_id = row.get('request_id')
        actual = issued_counts.get(request_id, 0) if isinstance(request_id, str) else 0
        if row.get('units_fulfilled') != actual:
            problem(f"blood_requests/{row.get('_id')}: stale fulfillment summary")
        demand = row.get('units_requested')
        if isinstance(demand, int) and not isinstance(demand, bool):
            status = 'Fulfilled' if actual == demand else 'Partially Fulfilled' if actual else 'Pending'
            if actual > demand or row.get('request_status') != status:
                problem(f"blood_requests/{row.get('_id')}: overfulfilled or invalid status summary")
    if len(donor_map) != len(donors) or len(request_map) != len(requests):
        problem('Missing or duplicated donor/request readable IDs')
    if not total_errors:
        result = analytics.dashboard(db, now=now)
        if {r['blood_group']: r['count'] for r in result['available_by_group']} != {
                group: stock[group] for group in BLOOD_GROUPS}:
            problem('Aggregation stock does not match independent record count')
        if {r['status']: r['count'] for r in result['inventory_status']} != dict(statuses):
            problem('Aggregation status totals do not match independent record count')
        if sum(row['count'] for row in result['monthly_donations']) != donations:
            problem('Monthly donations do not sum to all donations')
        if sum(row['count'] for row in result['requested_by_group']) != sum(r['units_requested'] for r in requests):
            problem('Requested-unit aggregation does not match independent sum')
        request_months = Counter()
        for row in requests:
            request_months[row['request_date'].strftime('%Y-%m')] += row['units_requested']
        if {r['month']: r['count'] for r in result['monthly_requests']} != dict(request_months):
            problem('Monthly requested units differ from independent sums')
        if {r['month']: r['count'] for r in result['monthly_donations']} != dict(donation_months):
            problem('Monthly donation buckets differ from independent counts')
        if {r['blood_group']: r['count'] for r in result['donation_distribution']} != dict(donation_groups):
            problem('Donation distribution differs from independent counts')
        cities = Counter(row['city'] for row in donors)
        expected_cities = sorted(cities.items(), key=lambda item: (-item[1], item[0]))[:10]
        if [(r['city'], r['count']) for r in result['top_cities']] != expected_cities:
            problem('Top donor cities differ from independent counts')
        expected_kpis = {'Total Registered Donors': len(donors), 'Total Donations': donations,
                         'Currently Available Blood Units': sum(stock.values()),
                         'Pending Blood Requests': sum(issued_counts[row['request_id']] < row['units_requested'] for row in requests),
                         'Expired Blood Units': statuses['Expired'], 'Blood Units Near Expiration': near_expiry}
        if result['kpis'] != expected_kpis:
            problem('Dashboard KPI values differ from independent counts')
    return {'ok': total_errors == 0, 'counts': {'donors': len(donors), 'donations': donations,
            'blood_requests': len(requests)}, 'effective_statuses': dict(statuses),
            'stale_available_flags': stale_statuses, 'error_count': total_errors, 'errors': errors}


def scale_check(sizes):
    results = []
    with TemporaryDirectory(prefix='blood_bank_spark_test_') as folder:
        db = Store(folder)
        for size in sizes:
            started = perf_counter()
            data = generate_dataset(size)
            insert_dataset(db, data, replace=True)
            inserted = perf_counter()
            audit = audit_database(db)
            results.append({'donations': size, 'total_records': sum(len(r) for r in data.values()),
                            'generate_and_save_seconds': round(inserted - started, 3),
                            'audit_seconds': round(perf_counter() - inserted, 3), 'audit': audit})
            print(f'Checked {size}: {audit["ok"]}', flush=True)
    return results

def main():
    parser = argparse.ArgumentParser(description='Read-only Spark/Parquet operational audit.')
    parser.add_argument('--scale', action='store_true')
    parser.add_argument('--sizes', type=int, nargs='+', default=[1000, 5000, 10000, 25000])
    args = parser.parse_args()
    if args.scale:
        if any(not 20 <= size <= 100000 for size in args.sizes):
            parser.error('Scale sizes must be 20 through 100000.')
        result = scale_check(args.sizes)
        ok = all(row['audit']['ok'] for row in result)
    else:
        result = audit_database(connect())
        ok = result['ok']
    print(json.dumps(result, indent=2))
    return 0 if ok else 1

if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, StorageError, OSError) as exc:
        raise SystemExit(str(exc)) from None
