"""Validated operations over LOCAL atomic Parquet snapshots.

All mutations lock, read a committed snapshot, validate, then publish once.
Parquet itself supplies no unique constraints or transactional row updates.
"""
import re
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4
from config import BLOOD_GROUPS, GENDERS, SHELF_LIFE_DAYS, utcnow
from database import sql_literal

def text_field(value, label, maximum=100):
    if not isinstance(value, str) or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f'{label} must be text without control characters.')
    value = value.strip()
    if not 2 <= len(value) <= maximum:
        raise ValueError(f'{label} must contain 2–{maximum} characters.')
    return value


def readable_id(value, prefix, generate=True):
    if not isinstance(value, str):
        raise ValueError('IDs must be text.')
    value = value.strip().upper()
    if not value and generate:
        value = f'{prefix}-{uuid4().hex[:12].upper()}'
    if not re.fullmatch(r'[A-Z0-9][A-Z0-9_-]{2,39}', value):
        raise ValueError('IDs must be 3–40 letters, digits, underscores or hyphens.')
    return value


def blood_group(value):
    if value not in BLOOD_GROUPS:
        raise ValueError('Select one of the eight supported blood groups.')
    return value


def integer(value, label, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f'{label} must be an integer from {minimum} to {maximum}.')
    return value


def as_utc(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, time.min, timezone.utc)
    raise ValueError('A valid date is required.')


def historical_date(value):
    value = as_utc(value)
    if value > utcnow():
        raise ValueError('Dates cannot be in the future.')
    return value



TABLES = ('donors', 'donations', 'blood_requests')

def state(db, snapshot=None):
    snapshot = db.manifest() if snapshot is None else snapshot
    return {name: db.rows(name, snapshot) for name in TABLES}

def summarize(data):
    counts = Counter(row['request_id'] for row in data['donations'] if row['unit_status'] == 'Issued')
    for row in data['blood_requests']:
        actual = counts[row['request_id']]
        row['units_fulfilled'] = actual
        row['request_status'] = ('Fulfilled' if actual >= row['units_requested']
                                 else 'Partially Fulfilled' if actual else 'Pending')

def validate_state(data, now=None):
    """Reject corruption before any operational snapshot is published."""
    now = now or utcnow()
    try:
        for table, keys in [('donors', ('donor_id',)), ('donations', ('donation_id', 'unit_id')),
                            ('blood_requests', ('request_id',))]:
            rows = data[table]
            for key in ('_id', *keys):
                values = [row[key] for row in rows]
                if any(not isinstance(value, str) or not value for value in values) or len(set(values)) != len(values):
                    raise ValueError(f'Duplicate or invalid {table}.{key}.')
            for row in rows:
                blood_group(row['blood_group'])
        donors = {row['donor_id']: row for row in data['donors']}
        requests = {row['request_id']: row for row in data['blood_requests']}
        for row in donors.values():
            readable_id(row['donor_id'], 'DNR', False)
            text_field(row['name'], 'Name')
            text_field(row['city'], 'City')
            integer(row['age'], 'Age', 18, 65)
            if row['gender'] not in GENDERS or not re.fullmatch(r'[0-9]{10}', row['contact_number']):
                raise ValueError('Invalid donor demographics/contact.')
            if as_utc(row['registration_date']) > now:
                raise ValueError('Future registration.')
        issued = Counter()
        for row in data['donations']:
            readable_id(row['donation_id'], 'DON', False)
            readable_id(row['unit_id'], 'UNT', False)
            donor = donors.get(row['donor_id'])
            if not donor or donor['blood_group'] != row['blood_group']:
                raise ValueError('Missing donor or blood-group mismatch.')
            donated, expires = as_utc(row['donation_date']), as_utc(row['expiration_date'])
            if not as_utc(donor['registration_date']) <= donated <= now or expires <= donated:
                raise ValueError('Invalid donation/expiration chronology.')
            if row['unit_status'] == 'Issued':
                request = requests.get(row['request_id'])
                if not request or request['blood_group'] != row['blood_group']:
                    raise ValueError('Missing request or blood-group mismatch.')
                at = as_utc(row['issued_at'])
                if not max(donated, as_utc(request['request_date'])) <= at < expires or at > now:
                    raise ValueError('Invalid issue timestamp / expired issuance.')
                issued[row['request_id']] += 1
            else:
                if row['unit_status'] not in ('Available', 'Expired') or row['request_id'] is not None or row['issued_at'] is not None:
                    raise ValueError('Invalid unissued status/metadata.')
                if row['unit_status'] == 'Expired' and expires > now:
                    raise ValueError('Premature Expired status.')
        for row in requests.values():
            readable_id(row['request_id'], 'REQ', False)
            text_field(row['hospital_name'], 'Hospital')
            integer(row['units_requested'], 'Demand', 1, 10000)
            integer(row['units_fulfilled'], 'Fulfilled', 0, row['units_requested'])
            if as_utc(row['request_date']) > now:
                raise ValueError('Future request.')
            actual = issued[row['request_id']]
            expected = 'Fulfilled' if actual == row['units_requested'] else 'Partially Fulfilled' if actual else 'Pending'
            if row['units_fulfilled'] != actual or row['request_status'] != expected:
                raise ValueError('Inconsistent request fulfillment summary.')
    except (KeyError, TypeError, AttributeError):
        raise ValueError('Malformed operational record; restore valid data.') from None
    return True

def save(db, data):
    validate_state(data)
    return db.publish(data)

def insert_record(db, table, row):
    row['_id'] = uuid4().hex
    with db.locked():
        data = state(db)
        data[table].append(row)
        save(db, data)
    return row

def add_donor(db, name, age, gender, group, city, contact_number, registration_date, donor_id=''):
    if gender not in GENDERS:
        raise ValueError('Select a valid gender option.')
    if not isinstance(contact_number, str) or not re.fullmatch(r'[0-9]{10}', contact_number.strip()):
        raise ValueError('Use a 10-digit synthetic contact number.')
    return insert_record(db, 'donors', {
        'donor_id': readable_id(donor_id, 'DNR'), 'name': text_field(name, 'Name'),
        'age': integer(age, 'Age', 18, 65), 'gender': gender, 'blood_group': blood_group(group),
        'city': text_field(city, 'City'), 'contact_number': contact_number.strip(),
        'registration_date': historical_date(registration_date)})

def add_donation(db, donor_id, donation_date, unit_id='', donation_id=''):
    donor_id = readable_id(donor_id, 'DNR', False)
    with db.locked():
        data = state(db)
        donor = next((r for r in data['donors'] if r['donor_id'] == donor_id), None)
        if not donor:
            raise ValueError('Donor ID does not exist. Register the donor first.')
        donated = historical_date(donation_date)
        if donated < as_utc(donor['registration_date']):
            raise ValueError('Donation date cannot precede donor registration.')
        expires = donated + timedelta(days=SHELF_LIFE_DAYS)
        row = {'_id': uuid4().hex, 'donation_id': readable_id(donation_id, 'DON'),
               'unit_id': readable_id(unit_id, 'UNT'), 'donor_id': donor_id,
               'blood_group': donor['blood_group'], 'donation_date': donated, 'expiration_date': expires,
               'unit_status': 'Expired' if expires <= utcnow() else 'Available', 'request_id': None, 'issued_at': None}
        data['donations'].append(row)
        save(db, data)
        return row

def add_request(db, hospital_name, group, units_requested, request_date, request_id=''):
    return insert_record(db, 'blood_requests', {
        'request_id': readable_id(request_id, 'REQ'), 'hospital_name': text_field(hospital_name, 'Hospital name'),
        'blood_group': blood_group(group), 'units_requested': integer(units_requested, 'Units requested', 1, 10000),
        'units_fulfilled': 0, 'request_date': historical_date(request_date), 'request_status': 'Pending'})

def is_available(row, now):
    return (row['unit_status'] == 'Available' and row['request_id'] is None
            and as_utc(row['donation_date']) <= now < as_utc(row['expiration_date']))

def timestamp_sql(value):
    return "TIMESTAMP " + sql_literal(as_utc(value).strftime('%Y-%m-%d %H:%M:%S.%f'))

def available_sql(now=None, group=None):
    at = timestamp_sql(now or utcnow())
    value = f"unit_status = 'Available' AND request_id IS NULL AND expiration_date > {at} AND donation_date <= {at}"
    if group:
        value += ' AND blood_group = ' + sql_literal(blood_group(group))
    return value

def mark_expired(db, now=None):
    now = now or utcnow()
    with db.locked():
        data = state(db)
        changed = 0
        for row in data['donations']:
            if row['unit_status'] == 'Available' and as_utc(row['expiration_date']) <= now:
                row['unit_status'] = 'Expired'
                changed += 1
        if changed:
            save(db, data)
        return changed

def reconcile_requests(db, request_id=None):
    with db.locked():
        data = state(db)
        before = [(r['units_fulfilled'], r['request_status']) for r in data['blood_requests']]
        summarize(data)
        if before != [(r['units_fulfilled'], r['request_status']) for r in data['blood_requests']]:
            save(db, data)

def fulfill_request(db, request_id, max_units=None):
    request_id = readable_id(request_id, 'REQ', False)
    if max_units is not None:
        integer(max_units, 'Issue limit', 1, 10000)
    with db.locked():
        data = state(db)
        summarize(data)
        validate_state(data)
        request = next((r for r in data['blood_requests'] if r['request_id'] == request_id), None)
        if request is None:
            raise ValueError('Request ID does not exist.')
        remaining = request['units_requested'] - request['units_fulfilled']
        if max_units is not None:
            remaining = min(remaining, max_units)
        at = utcnow()
        eligible = sorted((r for r in data['donations'] if is_available(r, at)
                           and r['blood_group'] == request['blood_group']),
                          key=lambda r: (r['expiration_date'], r['unit_id']))
        issued = []
        for row in eligible[:remaining]:
            at = utcnow()  # recheck immediately before including each unit in the issue snapshot
            if not is_available(row, at):
                continue
            row.update(unit_status='Issued', request_id=request_id, issued_at=at)
            issued.append(row['unit_id'])
        if issued:
            summarize(data)
            save(db, data)  # inventory AND request summaries committed in one pointer swap
        return issued

def list_records(db, table, where='true', order='1', skip=0, limit=100, snapshot=None):
    if table not in TABLES and table != 'daily_donations':
        raise ValueError('Unknown table.')
    integer(skip, 'Offset', 0, 1000000000)
    integer(limit, 'Page size', 1, 1000)
    snapshot = db.manifest() if snapshot is None else snapshot
    count = db.query(f'SELECT COUNT(*) AS count FROM {table} WHERE {where}', snapshot)[0]['count']
    rows = db.query(f'SELECT * FROM {table} WHERE {where} ORDER BY {order} LIMIT {limit} OFFSET {skip}', snapshot)
    return count, rows

def list_donors(db, group=None, city='', skip=0, limit=100):
    if not isinstance(city, str) or len(city) > 100:
        raise ValueError('City search must be text of at most 100 characters.')
    where = 'blood_group = ' + sql_literal(blood_group(group)) if group else 'true'
    if city.strip():
        where += ' AND contains(lower(city), ' + sql_literal(city.strip().lower()) + ')'
    return list_records(db, 'donors', where, 'donor_id', skip, limit)

def list_units(db, group=None, status='All', skip=0, limit=100):
    where = 'blood_group = ' + sql_literal(blood_group(group)) if group else 'true'
    if status == 'Available':
        where = available_sql(group=group)
    elif status != 'All':
        if status not in ('Issued', 'Expired'):
            raise ValueError('Invalid unit status.')
        where += ' AND unit_status = ' + sql_literal(status)
    return list_records(db, 'donations', where, 'expiration_date, unit_id', skip, limit)

def list_requests(db, group=None, status='All', skip=0, limit=100):
    where = 'blood_group = ' + sql_literal(blood_group(group)) if group else 'true'
    if status != 'All':
        if status not in ('Pending', 'Partially Fulfilled', 'Fulfilled'):
            raise ValueError('Invalid request status.')
        where += ' AND request_status = ' + sql_literal(status)
    return list_records(db, 'blood_requests', where, 'request_date DESC, request_id', skip, limit)
