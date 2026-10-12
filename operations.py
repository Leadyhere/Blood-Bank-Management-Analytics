"""Validated writes and atomic FEFO issuing for ONE running app process.

Donation records are authoritative. Request counters are repairable summaries.
The process lock serializes Streamlit sessions, but is not a distributed lock.
"""
import re
from datetime import date, datetime, time, timedelta, timezone
from threading import RLock
from uuid import uuid4

from pymongo import ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError

from config import BLOOD_GROUPS, GENDERS, SHELF_LIFE_DAYS, utcnow

WRITE_LOCK = RLock()


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


def insert_unique(collection, document):
    try:
        collection.insert_one(document)
    except DuplicateKeyError:
        raise ValueError('This ID already exists. Choose a different ID.') from None
    return document


def add_donor(db, name, age, gender, group, city, contact_number,
              registration_date, donor_id=''):
    if gender not in GENDERS:
        raise ValueError('Select a valid gender option.')
    if not isinstance(contact_number, str):
        raise ValueError('Contact number must be a 10-digit synthetic text value.')
    contact = contact_number.strip()
    if not re.fullmatch(r'\d{10}', contact, flags=re.ASCII):
        raise ValueError('Use a 10-digit synthetic contact number, such as 0000000001.')
    document = {
        'donor_id': readable_id(donor_id, 'DNR'),
        'name': text_field(name, 'Name'),
        'age': integer(age, 'Age (demonstration range)', 18, 65),
        'gender': gender, 'blood_group': blood_group(group),
        'city': text_field(city, 'City'), 'contact_number': contact,
        'registration_date': historical_date(registration_date),
    }
    with WRITE_LOCK:
        return insert_unique(db.donors, document)


def add_donation(db, donor_id, donation_date, unit_id='', donation_id=''):
    with WRITE_LOCK:
        donor = db.donors.find_one({'donor_id': readable_id(donor_id, 'DNR', generate=False)})
        if not donor:
            raise ValueError('Donor ID does not exist. Register the donor first.')
        donated = historical_date(donation_date)
        if donated < as_utc(donor['registration_date']):
            raise ValueError('Donation date cannot precede donor registration.')
        expires = donated + timedelta(days=SHELF_LIFE_DAYS)
        return insert_unique(db.donations, {
            'donation_id': readable_id(donation_id, 'DON'),
            'unit_id': readable_id(unit_id, 'UNT'), 'donor_id': donor['donor_id'],
            'blood_group': donor['blood_group'], 'donation_date': donated,
            'expiration_date': expires,
            'unit_status': 'Expired' if expires <= utcnow() else 'Available',
            'request_id': None, 'issued_at': None,
        })


def add_request(db, hospital_name, group, units_requested, request_date, request_id=''):
    with WRITE_LOCK:
        return insert_unique(db.blood_requests, {
            'request_id': readable_id(request_id, 'REQ'),
            'hospital_name': text_field(hospital_name, 'Hospital name'),
            'blood_group': blood_group(group),
            'units_requested': integer(units_requested, 'Units requested', 1, 10000),
            'units_fulfilled': 0, 'request_date': historical_date(request_date),
            'request_status': 'Pending',
        })


def available_filter(now=None, group=None):
    query = {'unit_status': 'Available', 'request_id': None,
             'expiration_date': {'$gt': now or utcnow()}}
    if group:
        query['blood_group'] = blood_group(group)
    return query


def mark_expired(db, now=None):
    with WRITE_LOCK:
        return db.donations.update_many(
            {'unit_status': 'Available', 'expiration_date': {'$lte': now or utcnow()}},
            {'$set': {'unit_status': 'Expired'}}).modified_count


def request_pipeline(query=None):
    """Read fulfillment from linked units, even after an interrupted write."""
    return [
        {'$match': query or {}},
        {'$lookup': {
            'from': 'donations', 'localField': 'request_id', 'foreignField': 'request_id',
            'pipeline': [{'$match': {'unit_status': 'Issued'}}, {'$count': 'count'}],
            'as': '_issued',
        }},
        {'$set': {'units_fulfilled': {'$ifNull': [{'$arrayElemAt': ['$_issued.count', 0]}, 0]}}},
        {'$set': {'request_status': {'$switch': {'branches': [
            {'case': {'$gte': ['$units_fulfilled', '$units_requested']}, 'then': 'Fulfilled'},
            {'case': {'$gt': ['$units_fulfilled', 0]}, 'then': 'Partially Fulfilled'},
        ], 'default': 'Pending'}}}},
        {'$project': {'_issued': 0}},
    ]


def reconcile_requests(db, request_id=None):
    """Repair stored summaries on startup and after issuing; safe to repeat."""
    with WRITE_LOCK:
        query = {'request_id': request_id} if request_id else {}
        updates = []
        for row in db.blood_requests.aggregate(request_pipeline(query)):
            updates.append(UpdateOne({'_id': row['_id']}, {'$set': {
                'units_fulfilled': row['units_fulfilled'],
                'request_status': row['request_status'],
            }}))
            if len(updates) == 500:
                db.blood_requests.bulk_write(updates)
                updates = []
        if updates:
            db.blood_requests.bulk_write(updates)


def fulfill_request(db, request_id, max_units=None):
    """Issue only the outstanding quantity, using atomic per-unit FEFO claims.

    Retrying after an ambiguous network failure first recounts linked units.
    There is no cross-document transaction; run only one app process.
    """
    with WRITE_LOCK:
        request_id = readable_id(request_id, 'REQ', generate=False)
        request = next(iter(db.blood_requests.aggregate(
            request_pipeline({'request_id': request_id}))), None)
        if not request:
            raise ValueError('Request ID does not exist.')
        remaining = max(0, request['units_requested'] - request['units_fulfilled'])
        if max_units is not None:
            remaining = min(remaining, integer(max_units, 'Issue limit', 1, 10000))
        issued = []
        try:
            for _ in range(remaining):
                now = utcnow()
                query = available_filter(now, request['blood_group'])
                # Recheck using MongoDB's command-time clock: client clock lag
                # or a delay before the command must not admit an expired unit.
                query['$expr'] = {'$and': [
                    {'$gt': ['$expiration_date', '$$NOW']},
                    {'$lte': ['$donation_date', '$$NOW']},
                ]}
                unit = db.donations.find_one_and_update(
                    query,
                    [{'$set': {'unit_status': 'Issued', 'request_id': {'$literal': request_id},
                               'issued_at': '$$NOW'}}],
                    sort=[('expiration_date', 1), ('unit_id', 1)],
                    return_document=ReturnDocument.AFTER,
                )
                if unit is None:
                    break
                issued.append(unit['unit_id'])
        finally:
            # If MongoDB is unreachable, the next startup/read repairs or derives this.
            reconcile_requests(db, request_id)
        return issued


def list_donors(db, group=None, city='', skip=0, limit=100):
    integer(skip, 'Offset', 0, 1000000000)
    integer(limit, 'Page size', 1, 1000)
    if not isinstance(city, str) or len(city) > 100:
        raise ValueError('City search must be text of at most 100 characters.')
    query = {}
    if group:
        query['blood_group'] = blood_group(group)
    if city.strip():
        query['city'] = {'$regex': re.escape(city.strip()), '$options': 'i'}
    return (db.donors.count_documents(query),
            list(db.donors.find(query).sort('donor_id', 1).skip(skip).limit(limit)))
