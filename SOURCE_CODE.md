# Complete source code

Every executable source/configuration template is reproduced below, one file per labeled block. Paths are relative to the project/repository root. `.env` is intentionally omitted: copy `.env.example` locally and keep credentials private. Setup and documentation are linked from README.md.

## config.py

```python
"""Small, environment-based configuration; all database dates use UTC."""
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name('.env'))


def positive_setting(name, default, maximum=3650):
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        raise ValueError(f'{name} must be an integer from 1 to {maximum}.') from None
    if not 1 <= value <= maximum:
        raise ValueError(f'{name} must be an integer from 1 to {maximum}.')
    return value


MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
MONGODB_DB = os.getenv('MONGODB_DB', 'blood_bank_analytics')
SHELF_LIFE_DAYS = positive_setting('SHELF_LIFE_DAYS', 42)
LOW_STOCK_THRESHOLD = positive_setting('LOW_STOCK_THRESHOLD', 5, maximum=1000000)
NEAR_EXPIRY_DAYS = positive_setting('NEAR_EXPIRY_DAYS', 7)
BLOOD_GROUPS = ('A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-')
GENDERS = ('Female', 'Male', 'Other', 'Prefer not to say')


def utcnow():
    return datetime.now(timezone.utc)
```

## database.py

```python
"""Connection and a deliberately small set of indexes."""
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import MONGODB_DB, MONGODB_URI


def connect(database_name=None):
    client = None
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000,
                             connectTimeoutMS=5000, socketTimeoutMS=10000,
                             timeoutMS=10000, tz_aware=True)
        client.admin.command('ping')
        db = client[database_name or MONGODB_DB]
        if not db.write_concern.acknowledged:
            raise ValueError('Acknowledged writes are required.')
        return client, db
    except (PyMongoError, ValueError, TypeError):
        if client is not None:
            client.close()
        # Do not echo the URI: it may contain credentials.
        raise ConnectionError('Cannot connect to MongoDB. Start the MongoDB '
                              'service, database name and MONGODB_URI in .env. '
                              'Acknowledged writes (w >= 1) are required.') from None


def create_indexes(db):
    db.donors.create_index('donor_id', unique=True)
    db.donors.create_index([('blood_group', 1), ('city', 1)])
    db.donations.create_index('donation_id', unique=True)
    db.donations.create_index('unit_id', unique=True)
    db.donations.create_index('donor_id')
    db.donations.create_index('donation_date')
    # Equality predicates first, then the FEFO sort / expiration range.
    db.donations.create_index([('blood_group', 1), ('unit_status', 1),
                               ('expiration_date', 1), ('unit_id', 1)],
                              name='inventory_fefo')
    db.donations.create_index([('unit_status', 1), ('expiration_date', 1)])
    db.donations.create_index('request_id')
    db.blood_requests.create_index('request_id', unique=True)
    db.blood_requests.create_index([('blood_group', 1), ('request_date', 1)])


if __name__ == '__main__':
    try:
        client, db = connect()
        create_indexes(db)
        print(f'Connected to {db.name}; indexes are ready.')
        for name in ('donors', 'donations', 'blood_requests'):
            print(f'{name}: {db[name].count_documents({}):,} documents')
        client.close()
    except (ConnectionError, PyMongoError) as exc:
        raise SystemExit(str(exc) if isinstance(exc, ConnectionError)
                         else 'Database operation failed. Check permissions and indexes.')
```

## operations.py

```python
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
```

## analytics.py

```python
"""MongoDB does the aggregation; Pandas/Plotly only format its results."""
import argparse
import json
from datetime import timedelta

from config import BLOOD_GROUPS, LOW_STOCK_THRESHOLD, NEAR_EXPIRY_DAYS, utcnow
from operations import as_utc, available_filter, blood_group, integer, request_pipeline


def history_filter(group=None, start=None, end=None, date_field='donation_date'):
    query = {}
    if group is not None:
        query['blood_group'] = blood_group(group)
    if (start is None) != (end is None):
        raise ValueError('Provide both a start and an end date.')
    if start is not None and end is not None:
        start, end = as_utc(start), as_utc(end)
        start, end = start.replace(hour=0, minute=0, second=0, microsecond=0), end.replace(hour=0, minute=0, second=0, microsecond=0)
        if start > end:
            raise ValueError('Start date must not be after end date.')
        query[date_field] = {'$gte': as_utc(start), '$lt': as_utc(end) + timedelta(days=1)}
    return query


def grouped_pipeline(query, field, value=1, limit=None):
    pipeline = [
        {'$match': query},
        {'$group': {'_id': f'${field}', 'count': {'$sum': value}}},
        {'$sort': {'count': -1, '_id': 1}},
        {'$project': {'_id': 0, field: '$_id', 'count': 1}},
    ]
    if limit:
        pipeline.append({'$limit': limit})
    return pipeline


def pipelines(now=None, group=None, start=None, end=None):
    now = now or utcnow()
    history = history_filter(group, start, end)
    current = {'blood_group': group} if group else {}
    expired = {**current, 'unit_status': {'$ne': 'Issued'},
               'expiration_date': {'$lte': now}}
    near = available_filter(now, group)
    near['expiration_date']['$lte'] = now + timedelta(days=NEAR_EXPIRY_DAYS)
    # Operational stock deliberately ignores the historical date filter.
    return {
        'available_by_group': ('donations', grouped_pipeline(available_filter(now, group), 'blood_group')),
        'monthly_donations': ('donations', [
            {'$match': history},
            {'$group': {'_id': {'$dateToString': {'format': '%Y-%m', 'date': '$donation_date',
                                                'timezone': 'UTC'}}, 'count': {'$sum': 1}}},
            {'$sort': {'_id': 1}},
            {'$project': {'_id': 0, 'month': '$_id', 'count': 1}},
        ]),
        'donation_distribution': ('donations', grouped_pipeline(history, 'blood_group')),
        'requested_by_group': ('blood_requests', grouped_pipeline(
            history_filter(group, start, end, 'request_date'), 'blood_group', '$units_requested')),
        'monthly_requests': ('blood_requests', [
            {'$match': history_filter(group, start, end, 'request_date')},
            {'$group': {'_id': {'$dateToString': {'format': '%Y-%m', 'date': '$request_date',
                                                'timezone': 'UTC'}}, 'count': {'$sum': '$units_requested'}}},
            {'$sort': {'_id': 1}}, {'$project': {'_id': 0, 'month': '$_id', 'count': 1}},
        ]),
        'top_cities': ('donors', grouped_pipeline(
            history_filter(group, start, end, 'registration_date'), 'city', limit=10)),
        'expired_count': ('donations', [{'$match': expired}, {'$count': 'count'}]),
        'near_expiry_count': ('donations', [{'$match': near}, {'$count': 'count'}]),
        'inventory_status': ('donations', [
            {'$match': current},
            {'$project': {'status': {'$switch': {'branches': [
                {'case': {'$eq': ['$unit_status', 'Issued']}, 'then': 'Issued'},
                {'case': {'$lte': ['$expiration_date', now]}, 'then': 'Expired'},
            ], 'default': '$unit_status'}}}},
            {'$group': {'_id': '$status', 'count': {'$sum': 1}}},
            {'$sort': {'_id': 1}}, {'$project': {'_id': 0, 'status': '$_id', 'count': 1}},
        ]),
    }


def all_group_rows(rows, group=None):
    counts = {row['blood_group']: row['count'] for row in rows}
    groups = (blood_group(group),) if group is not None else BLOOD_GROUPS
    return [{'blood_group': item, 'count': counts.get(item, 0)} for item in groups]


def dashboard(db, group=None, start=None, end=None, now=None):
    now = now or utcnow()
    results = {name: list(db[collection].aggregate(pipeline))
               for name, (collection, pipeline) in pipelines(now, group, start, end).items()}
    results['available_by_group'] = all_group_rows(results['available_by_group'], group)
    results['requested_by_group'] = all_group_rows(results['requested_by_group'], group)
    scope = {'blood_group': group} if group else {}
    open_pipeline = request_pipeline(scope) + [
        {'$match': {'request_status': {'$ne': 'Fulfilled'}}}, {'$count': 'count'}]
    pending = list(db.blood_requests.aggregate(open_pipeline))
    def count(rows):
        return rows[0]['count'] if rows else 0
    results['kpis'] = {
        'Total Registered Donors': db.donors.count_documents(scope),
        'Total Donations': db.donations.count_documents(scope),
        'Currently Available Blood Units': sum(row['count'] for row in results['available_by_group']),
        'Pending Blood Requests': count(pending),
        'Expired Blood Units': count(results['expired_count']),
        'Blood Units Near Expiration': count(results['near_expiry_count']),
    }
    results['low_stock'] = [row for row in results['available_by_group']
                            if (not group or row['blood_group'] == group)
                            and row['count'] < LOW_STOCK_THRESHOLD]
    return results


def expiration_units(db, near=True, group=None, skip=0, limit=100, now=None):
    integer(skip, 'Offset', 0, 1000000000)
    integer(limit, 'Page size', 1, 1000)
    if group is not None:
        blood_group(group)
    now = now or utcnow()
    if near:
        query = available_filter(now, group)
        query['expiration_date']['$lte'] = now + timedelta(days=NEAR_EXPIRY_DAYS)
    else:
        query = {'unit_status': {'$ne': 'Issued'}, 'expiration_date': {'$lte': now}}
        if group:
            query['blood_group'] = group
    return (db.donations.count_documents(query), list(db.donations.find(query).sort(
        [('expiration_date', 1), ('unit_id', 1)]).skip(skip).limit(limit)))


def explain_inventory(db, group='O+'):
    """Forced scan vs indexed access, without dropping existing indexes.

    The first run simulates the no-index access path; this is not a claim of
    having benchmarked a fresh database before any indexes existed.
    """
    from database import create_indexes
    now = utcnow()
    pipeline = grouped_pipeline(available_filter(now, group), 'blood_group')
    output = {}
    for label, hint in [('collection_scan', {'$natural': 1}), ('indexed', 'inventory_fefo')]:
        if label == 'indexed':
            create_indexes(db)
        raw = db.command('explain', {'aggregate': 'donations', 'pipeline': pipeline,
                                    'cursor': {}, 'hint': hint}, verbosity='executionStats')
        # Aggregation explain can contain a $cursor stage or top-level stats.
        stats = raw.get('executionStats')
        if stats is None:
            for stage in raw.get('stages', []):
                if '$cursor' in stage:
                    stats = stage['$cursor'].get('executionStats')
                    break
        output[label] = {'statistics': {key: (stats or {}).get(key) for key in (
            'executionTimeMillis', 'totalDocsExamined', 'totalKeysExamined', 'nReturned')},
            'raw_explain': raw}
    return output


if __name__ == '__main__':
    from database import connect
    parser = argparse.ArgumentParser(description='Inspect an inventory aggregation using explain().')
    parser.add_argument('--explain', action='store_true', required=True)
    parser.add_argument('--group', choices=BLOOD_GROUPS, default='O+')
    parser.add_argument('--raw', action='store_true')
    args = parser.parse_args()
    client, db = connect()
    try:
        output = explain_inventory(db, args.group)
        print(json.dumps(output if args.raw else {k: v['statistics'] for k, v in output.items()},
                         indent=2, default=str))
    finally:
        client.close()
```

## app.py

```python
"""Run with: python -m streamlit run app.py"""
import json
import math
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo.errors import PyMongoError

st.set_page_config(page_title='Blood Bank Analytics', page_icon='🩸', layout='wide')

try:
    import analytics
    import operations as ops
    from config import (BLOOD_GROUPS, GENDERS, LOW_STOCK_THRESHOLD,
                        NEAR_EXPIRY_DAYS, SHELF_LIFE_DAYS, utcnow)
    from database import connect, create_indexes
except ValueError:
    st.error('Invalid configuration. Shelf life and near-expiry days must be integers from 1 to 3650; '
             'the low-stock threshold must be an integer from 1 to 1000000. Check .env and restart.')
    st.stop()


@st.cache_resource
def database_connection():
    client, db = connect()
    try:
        create_indexes(db)
        ops.reconcile_requests(db)
    except Exception:
        client.close()
        raise
    return client, db


def table(rows):
    if not rows:
        st.info('No matching records. Add records or generate the demonstration dataset.')
        return
    frame = pd.DataFrame(rows).drop(columns=['_id'], errors='ignore')
    st.dataframe(frame, hide_index=True, width='stretch')


def offset(total, key):
    pages = max(1, math.ceil(total / 100))
    # Reset a page that is now out of range after changing a filter.
    if st.session_state.get(key, 1) > pages:
        st.session_state[key] = 1
    page = st.number_input('Page (100 rows per page)', min_value=1, max_value=pages,
                           step=1, key=key)
    st.caption(f'{total:,} matching records • {pages:,} page(s)')
    return (page - 1) * 100


def group_selector(label, key):
    selected = st.selectbox(label, ['All', *BLOOD_GROUPS], key=key)
    return None if selected == 'All' else selected


def write_action(function, *args, success='Saved.', **kwargs):
    try:
        result = function(*args, **kwargs)
        st.session_state['notice'] = success(result) if callable(success) else success
        st.rerun()
    except ValueError as exc:
        st.error(str(exc))
    except PyMongoError:
        st.error('MongoDB could not complete the operation. Some writes may have succeeded. '
                 'Reconnect and inspect the records before retrying. Issued units remain linked to their request.')


def chart(rows, x, title, kind='bar'):
    st.subheader(title)
    if not rows or not any(row['count'] for row in rows):
        st.info('No data for this chart and selection.')
        return
    frame = pd.DataFrame(rows)
    if kind == 'line':
        # Fill internal missing months with zero, without inventing observations.
        frame.index = pd.to_datetime(frame[x])
        frame = frame.reindex(pd.date_range(frame.index.min(), frame.index.max(), freq='MS'))
        frame['count'] = frame['count'].fillna(0)
        frame[x] = frame.index.strftime('%Y-%m')
        fig = px.line(frame, x=x, y='count', markers=True)
    elif kind == 'pie':
        fig = px.pie(frame, names=x, values='count', hole=.35)
    else:
        fig = px.bar(frame, x=x, y='count', text_auto=True)
    fig.update_layout(height=340, margin=dict(l=10, r=10, t=20, b=10),
                      yaxis_title='Units / records', xaxis_title=x.replace('_', ' ').title())
    st.plotly_chart(fig, width='stretch', key=f'chart_{title}')


def alert_tables(db, group=None, detailed=False):
    for near, label in [(True, f'Unissued units expiring within {NEAR_EXPIRY_DAYS} days'),
                        (False, 'Expired unissued units')]:
        total, rows = analytics.expiration_units(db, near, group)
        with st.expander(f'{label} ({total:,})'):
            if detailed:
                skip = offset(total, f'expiry_page_{near}')
                _, rows = analytics.expiration_units(db, near, group, skip=skip)
            elif total > 100:
                st.caption('Showing the first 100. Browse all records on Blood Inventory.')
            table(rows)


def report_page(db, detailed=False):
    st.title('Detailed Analytics' if detailed else 'Blood Bank Dashboard')
    group = group_selector('Blood group', 'report_group')
    apply_dates = st.checkbox('Filter historical charts by date', value=False)
    start = end = None
    if apply_dates:
        selected = st.date_input('Historical date range (UTC, inclusive)',
                                 value=(utcnow().date() - timedelta(days=365), utcnow().date()),
                                 min_value=date(1900, 1, 1), max_value=utcnow().date())
        if len(selected) != 2:
            st.info('Select both a start and an end date.')
            return
        start, end = selected
    st.caption('KPIs, inventory status and alerts show current/all-time values for the selected blood group. '
               'Date range filters donation, request and donor-registration charts only. '
               'Pending requests includes partially fulfilled requests. All dates are UTC.')
    result = analytics.dashboard(db, group, start, end)
    labels = list(result['kpis'].items())
    for row in (labels[:3], labels[3:]):
        for column, (label, value) in zip(st.columns(3), row):
            column.metric(label, f'{value:,}')
    if result['low_stock']:
        message = ', '.join(f"{row['blood_group']}: {row['count']}" for row in result['low_stock'])
        st.warning(f'Low stock (below {LOW_STOCK_THRESHOLD} units): {message}')
    else:
        st.success('Selected blood groups meet the stock threshold.')
    alert_tables(db, group)
    charts = [
        ('available_by_group', 'blood_group', 'Blood Group Inventory', 'bar'),
        ('monthly_donations', 'month', 'Monthly Donation Trends', 'line'),
        ('donation_distribution', 'blood_group', 'Blood Group Distribution', 'pie'),
        ('requested_by_group', 'blood_group', 'Blood Requests by Blood Group', 'bar'),
        ('inventory_status', 'status', 'Inventory Status Distribution', 'pie'),
        ('top_cities', 'city', 'Top Donor Cities', 'bar'),
    ]
    for position in range(0, len(charts), 2):
        for column, (name, x, title, kind) in zip(st.columns(2), charts[position:position + 2]):
            with column:
                chart(result[name], x, title, kind)
    if group:
        st.caption('Only the selected blood group is shown. Select All to compare all eight groups.')
    if detailed:
        chart(result['monthly_requests'], 'month', 'Monthly Requested Units', 'line')
        st.subheader('Dataset statistics (entire database)')
        stats = db.command('dbStats')
        table([{'collection': name, 'documents': db[name].count_documents({}),
                'indexes': len(db[name].index_information())}
               for name in ('donors', 'donations', 'blood_requests')])
        st.caption(f"Logical data size: {stats.get('dataSize', 0):,} bytes; "
                   f"index size: {stats.get('indexSize', 0):,} bytes. Sizes reported by MongoDB.")
        st.subheader('Aggregation pipelines and actual outputs')
        for name, (collection, pipeline) in analytics.pipelines(group=group, start=start, end=end).items():
            with st.expander(name.replace('_', ' ').title()):
                st.caption(f'Collection: {collection}')
                st.code(json.dumps(pipeline, indent=2, default=str), language='json')
                table(list(db[collection].aggregate(pipeline)))
        st.subheader('Index demonstration')
        st.caption('Compares a forced collection scan with the inventory index. No indexes are dropped. '
                   'Milliseconds can be zero for small datasets; compare examined documents too.')
        if st.button('Run explain comparison'):
            explanation = analytics.explain_inventory(db, group or 'O+')
            table([{'access_path': key, **value['statistics']} for key, value in explanation.items()])
            with st.expander('Raw explain output'):
                st.json(json.dumps(explanation, default=str))


def donor_page(db):
    st.title('Donor Management')
    st.metric('Total registered donors', db.donors.count_documents({}))
    with st.expander('Register a synthetic donor'):
        with st.form('donor_form'):
            donor_id = st.text_input('Donor ID (blank = automatic)')
            name = st.text_input('Name')
            age = st.number_input('Age (demonstration range)', 18, 65, 25)
            gender = st.selectbox('Gender', GENDERS)
            group = st.selectbox('Blood group', BLOOD_GROUPS)
            city = st.text_input('City')
            contact = st.text_input('Synthetic contact number', value='0000000001')
            registered = st.date_input('Registration date', value=utcnow().date(),
                                       min_value=date(1900, 1, 1), max_value=utcnow().date())
            if st.form_submit_button('Register donor'):
                write_action(ops.add_donor, db, name, age, gender, group, city, contact,
                             registered, donor_id, success=lambda row: f"Registered {row['donor_id']}.")
    group = group_selector('Search by blood group', 'donor_group')
    city = st.text_input('Search city (contains)')
    total, _ = ops.list_donors(db, group, city, limit=1)
    skip = offset(total, 'donor_page')
    _, rows = ops.list_donors(db, group, city, skip)
    table(rows)


def inventory_page(db):
    st.title('Blood Inventory')
    st.caption(f'One donation = one standardized demonstration red blood cell unit. '
               f'Shelf life: {SHELF_LIFE_DAYS} days. Actual component storage rules vary.')
    with st.expander('Record a donation'):
        with st.form('donation_form'):
            donor = st.text_input('Existing donor ID')
            unit_id = st.text_input('Unit ID (blank = automatic)')
            donation_id = st.text_input('Donation ID (blank = automatic)')
            donated = st.date_input('Donation date', value=utcnow().date(),
                                    min_value=date(1900, 1, 1), max_value=utcnow().date())
            if st.form_submit_button('Record donation'):
                write_action(ops.add_donation, db, donor, donated, unit_id, donation_id,
                             success=lambda row: f"Recorded {row['unit_id']} ({row['unit_status']}).")
    group = group_selector('Filter blood group', 'inventory_group')
    inventory_pipeline = analytics.grouped_pipeline(ops.available_filter(group=group), 'blood_group')
    table(analytics.all_group_rows(list(db.donations.aggregate(inventory_pipeline)), group))
    status = st.selectbox('Unit status', ['All', 'Available', 'Issued', 'Expired'])
    query = {'blood_group': group} if group else {}
    if status == 'Available':
        query = ops.available_filter(group=group)
    elif status != 'All':
        query['unit_status'] = status
    total = db.donations.count_documents(query)
    skip = offset(total, 'inventory_page')
    table(list(db.donations.find(query).sort([('expiration_date', 1), ('unit_id', 1)]).skip(skip).limit(100)))
    st.caption('Expiration statuses refresh on each interaction. Use Refresh data in the sidebar '
               'after leaving the app idle. Units are issued only through Blood Requests.')
    alert_tables(db, group, detailed=True)


def requests_page(db):
    st.title('Blood Requests')
    with st.expander('Add a hospital request'):
        with st.form('request_form'):
            request_id = st.text_input('Request ID (blank = automatic)')
            hospital = st.text_input('Synthetic hospital name')
            group = st.selectbox('Requested blood group', BLOOD_GROUPS)
            quantity = st.number_input('Units requested', 1, 10000, 1)
            requested = st.date_input('Request date', value=utcnow().date(),
                                      min_value=date(1900, 1, 1), max_value=utcnow().date())
            if st.form_submit_button('Add request'):
                write_action(ops.add_request, db, hospital, group, quantity, requested, request_id,
                             success=lambda row: f"Created {row['request_id']}.")
    with st.form('fulfillment_form'):
        st.subheader('Fulfill a request')
        request_id = st.text_input('Request ID to fulfill')
        limit = st.number_input('Maximum units to issue this time', 1, 10000, 10)
        st.caption('Exact blood-group match; earliest expiration first. Outstanding demand and '
                   'available inventory cap the quantity automatically.')
        if st.form_submit_button('Issue available units'):
            write_action(ops.fulfill_request, db, request_id, limit,
                         success=lambda units: (f'Issued {len(units)} unit(s). '
                                                'The table shows the updated request status.'))
    group = group_selector('Filter requested blood group', 'request_group')
    status = st.selectbox('Request status', ['All', 'Pending', 'Partially Fulfilled', 'Fulfilled'])
    pipeline = ops.request_pipeline({'blood_group': group} if group else {})
    if status != 'All':
        pipeline.append({'$match': {'request_status': status}})
    counts = list(db.blood_requests.aggregate(pipeline + [{'$count': 'count'}]))
    total = counts[0]['count'] if counts else 0
    skip = offset(total, 'request_page')
    table(list(db.blood_requests.aggregate(pipeline + [
        {'$sort': {'request_date': -1, 'request_id': 1}}, {'$skip': skip}, {'$limit': 100}])))


def main():
    st.sidebar.title('Blood Bank Analytics')
    page = st.sidebar.radio('Navigation', ['Dashboard', 'Donor Management', 'Blood Inventory',
                                            'Blood Requests', 'Analytics'])
    st.sidebar.caption('College mini project • Synthetic data only')
    st.sidebar.button('Refresh data')
    st.sidebar.info('Run one application process. Educational use only; no clinical decisions.')
    try:
        _, db = database_connection()
        ops.mark_expired(db)
        if 'notice' in st.session_state:
            st.success(st.session_state.pop('notice'))
        if page == 'Dashboard':
            report_page(db)
        elif page == 'Donor Management':
            donor_page(db)
        elif page == 'Blood Inventory':
            inventory_page(db)
        elif page == 'Blood Requests':
            requests_page(db)
        else:
            report_page(db, detailed=True)
    except ConnectionError as exc:
        st.error(str(exc))
        st.code('Get-Service MongoDB\n# In an Administrator terminal if stopped:\nStart-Service MongoDB')
    except PyMongoError:
        st.error('MongoDB operation failed. Check the server connection and database permissions, '
                 'then refresh. No credentials are displayed here.')


if __name__ == '__main__':
    main()
```

## seed_data.py

```python
"""Reproducible synthetic data and batched MongoDB insertion.

--records N means N DONATIONS, plus ceil(N/2) donors and ceil(N/5) requests.
No real people, telephone numbers or hospital identities are used.
"""
import argparse
import heapq
import math
import random
from collections import defaultdict
from datetime import datetime, time, timedelta, timezone

from bson import ObjectId
from faker import Faker
from pymongo.errors import PyMongoError

from config import BLOOD_GROUPS, GENDERS, SHELF_LIFE_DAYS, utcnow
from database import connect, create_indexes
from operations import as_utc, integer

CITIES = ('Mumbai', 'Delhi', 'Pune', 'Bengaluru', 'Chennai', 'Kolkata', 'Jaipur', 'Hyderabad')


def generate_dataset(records=5000, seed=42, now=None):
    integer(records, 'Donation records', 20, 1000000)
    now = as_utc(now) if now is not None else datetime.combine(utcnow().date(), time.min, timezone.utc)
    rng = random.Random(seed)
    fake = Faker('en_IN')
    fake.seed_instance(seed)
    def oid():
        return ObjectId(rng.getrandbits(96).to_bytes(12, 'big'))
    donors = []
    for i in range(math.ceil(records / 2)):
        donors.append({
            '_id': oid(), 'donor_id': f'DNR-{i + 1:06d}',
            'name': f'Synthetic {fake.name()}', 'age': rng.randint(18, 65),
            'gender': rng.choice(GENDERS), 'blood_group': BLOOD_GROUPS[i % 8],
            'city': rng.choice(CITIES), 'contact_number': f'000{i + 1:07d}',
            'registration_date': now - timedelta(days=rng.randint(400, 730)),
        })
    donations = []
    for i in range(records):
        donor = rng.choice(donors)
        # A recent cohort ensures usable stock even with a small demo dataset.
        days_ago = rng.randint(0, min(SHELF_LIFE_DAYS - 1, 365)) if i < records * .35 else rng.randint(1, 365)
        donated = now - timedelta(days=days_ago)
        donations.append({
            '_id': oid(), 'donation_id': f'DON-{i + 1:06d}', 'unit_id': f'UNT-{i + 1:06d}',
            'donor_id': donor['donor_id'], 'blood_group': donor['blood_group'],
            'donation_date': donated, 'expiration_date': donated + timedelta(days=SHELF_LIFE_DAYS),
            'unit_status': 'Available', 'request_id': None, 'issued_at': None,
        })
    requests = []
    for i in range(math.ceil(records / 5)):
        requests.append({
            '_id': oid(), 'request_id': f'REQ-{i + 1:06d}',
            'hospital_name': f'Synthetic {rng.choice(CITIES)} Hospital {i % 30 + 1}',
            'blood_group': BLOOD_GROUPS[i % 8], 'units_requested': rng.randint(1, 5),
            'units_fulfilled': 0, 'request_date': now - timedelta(days=rng.randint(0, 364)),
            'request_status': 'Pending',
        })
    # Replay historical requests in time order. Per-group heaps implement FEFO.
    ordered = sorted(donations, key=lambda row: (row['donation_date'], row['unit_id']))
    # Reserve a small waste sample for expiry demonstrations. These units are
    # deliberately never allocated, even if demand was present historically.
    # A long configured shelf life can legitimately leave no expired candidates.
    expired_candidates = [row for row in ordered if row['expiration_date'] <= now]
    reserved = {row['unit_id'] for row in expired_candidates[:max(1, records // 50)]}
    pools = defaultdict(list)
    cursor = 0
    for request in sorted(requests, key=lambda row: (row['request_date'], row['request_id'])):
        at = request['request_date']
        while cursor < len(ordered) and ordered[cursor]['donation_date'] <= at:
            unit = ordered[cursor]
            if unit['unit_id'] not in reserved:
                heapq.heappush(pools[unit['blood_group']], (unit['expiration_date'], unit['unit_id'], unit))
            cursor += 1
        pool = pools[request['blood_group']]
        while pool and pool[0][0] <= at:
            heapq.heappop(pool)
        while pool and request['units_fulfilled'] < request['units_requested']:
            _, _, unit = heapq.heappop(pool)
            unit.update(unit_status='Issued', request_id=request['request_id'], issued_at=at)
            request['units_fulfilled'] += 1
        fulfilled = request['units_fulfilled']
        request['request_status'] = ('Fulfilled' if fulfilled == request['units_requested']
                                     else 'Partially Fulfilled' if fulfilled else 'Pending')
    for unit in donations:
        if unit['unit_status'] != 'Issued' and unit['expiration_date'] <= now:
            unit['unit_status'] = 'Expired'
    return {'donors': donors, 'blood_requests': requests, 'donations': donations}


def insert_dataset(db, dataset, batch_size=1000):
    integer(batch_size, 'Batch size', 1, 10000)
    for collection in ('donors', 'blood_requests', 'donations'):
        rows = dataset[collection]
        for offset in range(0, len(rows), batch_size):
            db[collection].insert_many(rows[offset:offset + batch_size], ordered=True)
        print(f'Inserted {len(rows):,} {collection}.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--records', type=int, default=5000, help='Number of donation documents (minimum 20).')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--batch-size', type=int, default=1000)
    parser.add_argument('--reset', action='store_true', help='Prompt before dropping the three demo collections.')
    args = parser.parse_args()
    if not 20 <= args.records <= 1000000 or not 1 <= args.batch_size <= 10000:
        parser.error('Records must be 20–1,000,000 and batch size must be 1–10,000.')
    client, db = connect()
    try:
        names = ('donors', 'donations', 'blood_requests')
        counts = {name: db[name].count_documents({}) for name in names}
        if args.reset:
            print(f'Target database: {db.name}; existing document counts: {counts}')
            print('Stop Streamlit before resetting. Only the three named collections will be dropped.')
            expected = f'RESET {db.name}'
            if input(f'Type exactly "{expected}" to confirm deletion: ').strip() != expected:
                print('Cancelled. No data was deleted.')
                return
            # Generate before deleting anything, so generation errors preserve existing data.
            dataset = generate_dataset(args.records, args.seed)
            for name in names:
                db.drop_collection(name)
        elif any(counts.values()):
            raise ValueError('Database is not empty. No records changed. Use --reset and confirm to regenerate.')
        else:
            dataset = generate_dataset(args.records, args.seed)
        create_indexes(db)
        insert_dataset(db, dataset, args.batch_size)
        print(f'Done: {sum(len(rows) for rows in dataset.values()):,} total documents. '
              'Run: python -m streamlit run app.py')
    finally:
        client.close()


if __name__ == '__main__':
    try:
        main()
    except (ValueError, ConnectionError, EOFError) as exc:
        raise SystemExit(str(exc)) from None
    except PyMongoError:
        raise SystemExit('MongoDB write failed; some batches may have succeeded. '
                         'Inspect the database, then explicitly reset to retry.') from None
```

## test_project.py

```python
"""Standard-library tests. Set RUN_MONGO_TESTS=1 for real MongoDB/UI tests.

Integration tests create and remove only a random blood_bank_test_<uuid> DB.
They never delete the demonstration database.
"""
import os
import unittest
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4

import analytics
import operations as ops
from config import BLOOD_GROUPS, LOW_STOCK_THRESHOLD, SHELF_LIFE_DAYS, utcnow
from database import connect, create_indexes
from seed_data import generate_dataset, insert_dataset


class PureTests(unittest.TestCase):
    def test_validation(self):
        for value in ('C+', '', 'o+'):
            with self.assertRaises(ValueError):
                ops.blood_group(value)
        for value in (0, -1, 1.5, True):
            with self.assertRaises(ValueError):
                ops.integer(value, 'Quantity', 1, 100)
        with self.assertRaises(ValueError):
            ops.historical_date(utcnow() + timedelta(days=2))
        with self.assertRaises(ValueError):
            ops.readable_id('bad id', 'DNR')

    def test_zero_stock_groups(self):
        rows = analytics.all_group_rows([])
        self.assertEqual([row['blood_group'] for row in rows], list(BLOOD_GROUPS))
        self.assertEqual(sum(row['count'] for row in rows), 0)

    def test_inclusive_date_filter(self):
        today = utcnow().date()
        query = analytics.history_filter(start=today, end=today)
        self.assertEqual(query['donation_date']['$lt'] - query['donation_date']['$gte'], timedelta(days=1))

    def test_reproducible_generation(self):
        now = utcnow()
        self.assertEqual(generate_dataset(100, 42, now), generate_dataset(100, 42, now))

    def test_seed_integrity(self):
        now = utcnow()
        data = generate_dataset(1000, now=now)
        donors = {row['donor_id']: row for row in data['donors']}
        requests = {row['request_id']: row for row in data['blood_requests']}
        fulfilled = Counter()
        self.assertEqual(len(data['donations']), 1000)
        self.assertEqual(len(donors), 500)
        self.assertEqual(len(requests), 200)
        self.assertEqual(len({r['unit_id'] for r in data['donations']}), 1000)
        self.assertEqual(len({r['donation_id'] for r in data['donations']}), 1000)
        self.assertEqual(len({r['_id'] for rows in data.values() for r in rows}), 1700)
        for unit in data['donations']:
            donor = donors[unit['donor_id']]
            self.assertEqual(unit['blood_group'], donor['blood_group'])
            self.assertGreaterEqual(unit['donation_date'], donor['registration_date'])
            self.assertEqual(unit['expiration_date'] - unit['donation_date'], timedelta(days=SHELF_LIFE_DAYS))
            if unit['unit_status'] == 'Issued':
                request = requests[unit['request_id']]
                self.assertEqual(unit['blood_group'], request['blood_group'])
                self.assertLessEqual(unit['donation_date'], unit['issued_at'])
                self.assertLessEqual(request['request_date'], unit['issued_at'])
                self.assertLess(unit['issued_at'], unit['expiration_date'])
                fulfilled[unit['request_id']] += 1
            else:
                self.assertIsNone(unit['request_id'])
                self.assertEqual(unit['unit_status'], 'Expired' if unit['expiration_date'] <= now else 'Available')
        for request in requests.values():
            self.assertEqual(request['units_fulfilled'], fulfilled[request['request_id']])
            self.assertLessEqual(request['units_fulfilled'], request['units_requested'])
            expected = ('Fulfilled' if request['units_fulfilled'] == request['units_requested']
                        else 'Partially Fulfilled' if request['units_fulfilled'] else 'Pending')
            self.assertEqual(request['request_status'], expected)
        self.assertEqual({r['unit_status'] for r in data['donations']}, {'Available', 'Issued', 'Expired'})
        self.assertTrue(any(r['request_status'] == 'Partially Fulfilled' for r in requests.values()))


@unittest.skipUnless(os.getenv('RUN_MONGO_TESTS') == '1', 'Set RUN_MONGO_TESTS=1 to use real MongoDB.')
class MongoTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database_name = 'blood_bank_test_' + uuid4().hex
        cls.client, cls.db = connect(cls.database_name)
        create_indexes(cls.db)

    @classmethod
    def tearDownClass(cls):
        assert cls.db.name == cls.database_name and cls.database_name.startswith('blood_bank_test_')
        cls.client.drop_database(cls.database_name)
        cls.client.close()

    def setUp(self):
        for name in ('donors', 'donations', 'blood_requests'):
            self.db[name].delete_many({})
        self.now = utcnow()

    def donor(self, group='O+', donor_id='DNR-TEST'):
        return ops.add_donor(self.db, 'Synthetic Student', 25, 'Other', group, 'Pune',
                             '0000000001', self.now - timedelta(days=500), donor_id)

    def unit(self, unit_id, days_old=1, donor_id='DNR-TEST'):
        return ops.add_donation(self.db, donor_id, self.now - timedelta(days=days_old), unit_id)

    def request(self, quantity=2, group='O+', request_id='REQ-TEST'):
        return ops.add_request(self.db, 'Synthetic Hospital', group, quantity, self.now, request_id)

    def test_connection_and_empty_dashboard(self):
        self.assertEqual(self.client.admin.command('ping')['ok'], 1)
        result = analytics.dashboard(self.db)
        self.assertTrue(all(value == 0 for value in result['kpis'].values()))
        self.assertEqual(len(result['low_stock']), 8)

    def test_donor_search_validation_and_duplicates(self):
        self.donor()
        with self.assertRaises(ValueError):
            self.donor()
        self.assertEqual(ops.list_donors(self.db, 'O+', 'pun')[0], 1)
        self.assertEqual(ops.list_donors(self.db, city='.*')[0], 0)
        with self.assertRaises(ValueError):
            ops.add_donor(self.db, 'Valid Name', 25, 'Other', 'O+', 'Pune', '123', self.now)

    def test_donation_relationship_and_ids(self):
        with self.assertRaises(ValueError):
            self.unit('UNT-MISSING')
        self.donor('AB-')
        unit = self.unit('UNT-TEST')
        self.assertEqual(unit['blood_group'], 'AB-')
        with self.assertRaises(ValueError):
            self.unit('UNT-TEST')
        with self.assertRaises(ValueError):
            ops.add_donation(self.db, 'DNR-TEST', self.now, 'UNT-NEW', unit['donation_id'])
        with self.assertRaises(ValueError):
            self.unit('UNT-OLD', 600)

    def test_fefo_partial_fulfillment_and_expiry(self):
        self.donor()
        self.unit('UNT-LATER', 1)
        self.unit('UNT-FIRST', SHELF_LIFE_DAYS - 1)
        expired = self.unit('UNT-EXPIRED', SHELF_LIFE_DAYS + 1)
        # Even a stale Available flag must not make an expired unit issuable.
        self.db.donations.update_one({'_id': expired['_id']}, {'$set': {'unit_status': 'Available'}})
        self.request(3)
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST', 1), ['UNT-FIRST'])
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), ['UNT-LATER'])
        row = self.db.blood_requests.find_one({'request_id': 'REQ-TEST'})
        self.assertEqual((row['units_fulfilled'], row['request_status']), (2, 'Partially Fulfilled'))
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), [])
        self.assertEqual(self.db.donations.count_documents(ops.available_filter()), 0)
        self.assertEqual(ops.mark_expired(self.db), 1)
        self.assertEqual(analytics.dashboard(self.db)['kpis']['Expired Blood Units'], 1)

    def test_exact_group_and_complete_request(self):
        self.donor('O-')
        self.unit('UNT-TEST')
        self.request(1, 'O+')
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), [])
        self.request(1, 'O-', 'REQ-MATCH')
        self.assertEqual(len(ops.fulfill_request(self.db, 'REQ-MATCH')), 1)
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-MATCH'), [])
        self.assertEqual(self.db.blood_requests.find_one({'request_id': 'REQ-MATCH'})['request_status'], 'Fulfilled')

    def test_expiry_boundaries_and_issued_history(self):
        self.donor()
        for name in ('UNT-NOW', 'UNT-NEAR', 'UNT-LATER', 'UNT-ISSUED'):
            self.unit(name)
        for name, delta in [('UNT-NOW', 0), ('UNT-NEAR', 7), ('UNT-LATER', 8), ('UNT-ISSUED', -1)]:
            self.db.donations.update_one({'unit_id': name}, {'$set': {'expiration_date': self.now + timedelta(days=delta)}})
        self.db.donations.update_one({'unit_id': 'UNT-ISSUED'}, {'$set': {'unit_status': 'Issued'}})
        result = analytics.dashboard(self.db, now=self.now)
        self.assertEqual(result['kpis']['Expired Blood Units'], 1)
        self.assertEqual(result['kpis']['Blood Units Near Expiration'], 1)
        self.assertEqual(result['kpis']['Currently Available Blood Units'], 2)
        self.assertEqual(sum(row['count'] for row in result['inventory_status']), 4)

    def test_interrupted_summary_recovers_without_overissue(self):
        self.donor()
        self.unit('UNT-ONE')
        self.unit('UNT-TWO')
        self.request(1)
        with patch('operations.reconcile_requests', side_effect=RuntimeError('simulated interruption')):
            with self.assertRaises(RuntimeError):
                ops.fulfill_request(self.db, 'REQ-TEST')
        self.assertEqual(self.db.blood_requests.find_one({})['units_fulfilled'], 0)
        derived = list(self.db.blood_requests.aggregate(ops.request_pipeline()))[0]
        self.assertEqual(derived['units_fulfilled'], 1)
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), [])
        self.assertEqual(self.db.blood_requests.find_one({})['units_fulfilled'], 1)
        self.assertEqual(self.db.donations.count_documents(ops.available_filter()), 1)

    def test_two_sessions_cannot_overfulfill(self):
        self.donor()
        for i in range(5):
            self.unit(f'UNT-{i:03d}')
        self.request(2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: ops.fulfill_request(self.db, 'REQ-TEST'), range(2)))
        self.assertEqual(sum(len(rows) for rows in results), 2)
        self.assertEqual(self.db.donations.count_documents(ops.available_filter()), 3)

    def test_aggregation_and_explain(self):
        insert_dataset(self.db, generate_dataset(1000))
        result = analytics.dashboard(self.db)
        self.assertEqual(result['kpis']['Total Donations'], 1000)
        self.assertEqual(sum(row['count'] for row in result['monthly_donations']), 1000)
        self.assertEqual(sum(row['count'] for row in result['inventory_status']), 1000)
        expected = sum(row['units_requested'] for row in self.db.blood_requests.find({}))
        self.assertEqual(sum(row['count'] for row in result['requested_by_group']), expected)
        filtered = analytics.dashboard(self.db, start=self.now.date(), end=self.now.date())
        self.assertEqual(filtered['kpis'], result['kpis'])
        explanation = analytics.explain_inventory(self.db)
        for path in explanation.values():
            self.assertIsInstance(path['statistics']['totalDocsExamined'], int)
        self.assertLessEqual(explanation['indexed']['statistics']['totalDocsExamined'],
                             explanation['collection_scan']['statistics']['totalDocsExamined'])

    def test_stock_threshold(self):
        self.donor()
        for i in range(LOW_STOCK_THRESHOLD):
            self.unit(f'UNT-{i:03d}')
        self.assertNotIn('O+', [row['blood_group'] for row in analytics.dashboard(self.db)['low_stock']])
        self.request(1)
        ops.fulfill_request(self.db, 'REQ-TEST')
        self.assertIn('O+', [row['blood_group'] for row in analytics.dashboard(self.db)['low_stock']])

    def test_streamlit_pages_empty_and_seeded(self):
        import streamlit as st
        from streamlit.testing.v1 import AppTest
        for seeded in (False, True):
            if seeded:
                insert_dataset(self.db, generate_dataset(1000))
            st.cache_resource.clear()
            with patch('database.connect', return_value=(self.client, self.db)):
                app = AppTest.from_file('app.py', default_timeout=30).run()
                self.assertEqual(len(app.exception), 0)
                for page in ['Donor Management', 'Blood Inventory', 'Blood Requests', 'Analytics', 'Dashboard']:
                    app.sidebar.radio[0].set_value(page).run()
                    self.assertEqual(len(app.exception), 0, page)
                    self.assertEqual(len(app.error), 0, page)
        st.cache_resource.clear()

    def test_streamlit_forms_end_to_end(self):
        import streamlit as st
        from streamlit.testing.v1 import AppTest
        st.cache_resource.clear()
        with patch('database.connect', return_value=(self.client, self.db)):
            app = AppTest.from_file('app.py', default_timeout=30).run()
            def field(elements, label):
                return next(element for element in elements if element.label == label)
            app.sidebar.radio[0].set_value('Donor Management').run()
            field(app.text_input, 'Donor ID (blank = automatic)').set_value('DNR-UI')
            field(app.text_input, 'Name').set_value('Synthetic UI Donor')
            field(app.text_input, 'City').set_value('Pune')
            field(app.button, 'Register donor').click().run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(self.db.donors.count_documents({'donor_id': 'DNR-UI'}), 1)
            app.sidebar.radio[0].set_value('Blood Inventory').run()
            field(app.text_input, 'Existing donor ID').set_value('DNR-UI')
            field(app.text_input, 'Unit ID (blank = automatic)').set_value('UNT-UI')
            field(app.button, 'Record donation').click().run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(self.db.donations.count_documents(ops.available_filter()), 1)
            app.sidebar.radio[0].set_value('Blood Requests').run()
            field(app.text_input, 'Request ID (blank = automatic)').set_value('REQ-UI')
            field(app.text_input, 'Synthetic hospital name').set_value('Synthetic UI Hospital')
            field(app.button, 'Add request').click().run()
            field(app.text_input, 'Request ID to fulfill').set_value('REQ-UI')
            field(app.button, 'Issue available units').click().run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(len(app.error), 0)
            self.assertEqual(self.db.blood_requests.find_one({'request_id': 'REQ-UI'})['request_status'], 'Fulfilled')
            self.assertEqual(self.db.donations.count_documents(ops.available_filter()), 0)
            app.sidebar.radio[0].set_value('Dashboard').run()
            field(app.checkbox, 'Filter historical charts by date').check().run()
            self.assertEqual(len(app.exception), 0)
            app.sidebar.radio[0].set_value('Analytics').run()
            field(app.button, 'Run explain comparison').click().run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(len(app.error), 0)
        st.cache_resource.clear()


if __name__ == '__main__':
    unittest.main(verbosity=2)
```

## test_robustness.py

```python
"""Regression, outage, concurrency and seed-safety tests for the audit."""
import os
import subprocess
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

from pymongo.errors import AutoReconnect, ConfigurationError

import analytics
import database
import operations as ops
import seed_data
from config import positive_setting, utcnow
from verify_project import audit_database


class RobustPureTests(unittest.TestCase):
    def test_required_text_rejects_non_strings_and_controls(self):
        for value in (None, 123, [], {}, 'A\x00B', 'A\nB'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ops.text_field(value, 'Name')

    def test_ids_reject_non_strings_and_normalize_whitespace(self):
        for value in (None, 123, [], {}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ops.readable_id(value, 'DNR')
        self.assertTrue(ops.readable_id('   ', 'DNR').startswith('DNR-'))

    def test_history_filters_validate_group_and_both_dates(self):
        for kwargs in ({'group': 'INVALID'}, {'start': utcnow().date()}, {'end': utcnow().date()}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                analytics.history_filter(**kwargs)

    def test_filtered_groups_do_not_appear_as_false_zeroes(self):
        self.assertEqual(analytics.all_group_rows([{'blood_group': 'O+', 'count': 3}], 'O+'),
                         [{'blood_group': 'O+', 'count': 3}])

    def test_constructor_failure_is_sanitized(self):
        with patch('database.MongoClient', side_effect=ConfigurationError('private-uri-sentinel')):
            with self.assertRaises(ConnectionError) as caught:
                database.connect()
        self.assertNotIn('private-uri-sentinel', str(caught.exception))

    def test_failed_ping_closes_client(self):
        client = MagicMock()
        client.admin.command.side_effect = AutoReconnect('private-uri-sentinel')
        with patch('database.MongoClient', return_value=client):
            with self.assertRaises(ConnectionError) as caught:
                database.connect()
        client.close.assert_called_once()
        self.assertNotIn('private-uri-sentinel', str(caught.exception))

    def test_large_shelf_life_preserves_registration_order(self):
        with patch('seed_data.SHELF_LIFE_DAYS', 1000):
            data = seed_data.generate_dataset(1000)
        registered = {row['donor_id']: row['registration_date'] for row in data['donors']}
        self.assertTrue(all(row['donation_date'] >= registered[row['donor_id']]
                            for row in data['donations']))

    def test_invalid_seed_sizes_are_rejected(self):
        for size in (True, 19, 20.5, '100', None):
            with self.subTest(size=size), self.assertRaises(ValueError):
                seed_data.generate_dataset(size)

    def test_default_seed_all_states_at_25000(self):
        data = seed_data.generate_dataset(25000)
        self.assertEqual({row['unit_status'] for row in data['donations']}, {'Available', 'Issued', 'Expired'})

    def test_settings_reject_bad_or_extreme_values(self):
        for value in ('abc', '0', '-1', '99999999999999999'):
            with self.subTest(value=value), patch.dict(os.environ, {'BBA_TEST_SETTING': value}):
                with self.assertRaisesRegex(ValueError, 'BBA_TEST_SETTING'):
                    positive_setting('BBA_TEST_SETTING', 42)

    def test_client_uses_bounded_operation_timeout(self):
        client = MagicMock()
        with patch('database.MongoClient', return_value=client) as constructor:
            database.connect()
        self.assertEqual(constructor.call_args.kwargs['socketTimeoutMS'], 10000)
        self.assertEqual(constructor.call_args.kwargs['timeoutMS'], 10000)

    def test_invalid_database_name_closes_client(self):
        client = MagicMock()
        client.__getitem__.side_effect = ValueError('private-db-sentinel')
        with patch('database.MongoClient', return_value=client):
            with self.assertRaises(ConnectionError) as caught:
                database.connect()
        client.close.assert_called_once()
        self.assertNotIn('private-db-sentinel', str(caught.exception))

    def test_unacknowledged_writes_are_rejected(self):
        client = MagicMock()
        client.__getitem__.return_value.write_concern.acknowledged = False
        with patch('database.MongoClient', return_value=client):
            with self.assertRaises(ConnectionError):
                database.connect()
        client.close.assert_called_once()

    def test_pagination_rejects_unbounded_or_negative_limits(self):
        for kwargs in ({'limit': 0}, {'limit': -1}, {'skip': -1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                ops.list_donors(MagicMock(), **kwargs)

    def test_bad_configuration_displays_error_in_streamlit(self):
        code = ('from streamlit.testing.v1 import AppTest; '
                'app=AppTest.from_file("app.py", default_timeout=30).run(); '
                'assert len(app.exception)==0; assert len(app.error)==1; '
                'assert "Invalid configuration" in app.error[0].value')
        result = subprocess.run([sys.executable, '-c', code], env={**os.environ, 'SHELF_LIFE_DAYS': 'bad'},
                                text=True, capture_output=True, timeout=45)
        self.assertEqual(result.returncode, 0, result.stderr)


@unittest.skipUnless(os.getenv('RUN_MONGO_TESTS') == '1', 'Set RUN_MONGO_TESTS=1.')
class RobustMongoTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.name = 'blood_bank_test_' + uuid4().hex
        cls.client, cls.db = database.connect(cls.name)
        database.create_indexes(cls.db)

    @classmethod
    def tearDownClass(cls):
        assert cls.db.name == cls.name and cls.name.startswith('blood_bank_test_')
        cls.client.drop_database(cls.name)
        cls.client.close()

    def setUp(self):
        for collection in ('donors', 'donations', 'blood_requests'):
            self.db[collection].delete_many({})
        self.now = utcnow()
        ops.add_donor(self.db, 'Synthetic Robustness Donor', 25, 'Other', 'O+', 'Pune',
                      '0000000001', self.now - timedelta(days=400), 'DNR-ROBUST')

    def unit(self, name='UNT-ROBUST'):
        return ops.add_donation(self.db, 'DNR-ROBUST', self.now - timedelta(days=1), name)

    def request(self, name='REQ-ROBUST', quantity=1):
        return ops.add_request(self.db, 'Synthetic Test Hospital', 'O+', quantity, self.now, name)

    def test_application_clock_lag_cannot_issue_expired_unit(self):
        unit = self.unit()
        self.request()
        self.db.donations.update_one({'_id': unit['_id']}, {'$set': {
            'expiration_date': self.now - timedelta(seconds=1), 'unit_status': 'Available'}})
        with patch('operations.utcnow', return_value=self.now - timedelta(seconds=10)):
            self.assertEqual(ops.fulfill_request(self.db, 'REQ-ROBUST'), [])

    def test_request_id_duplicates(self):
        self.request()
        with self.assertRaises(ValueError):
            self.request()
        self.assertEqual(self.db.blood_requests.count_documents({}), 1)

    def test_donor_id_invalid_type_is_friendly(self):
        with self.assertRaises(ValueError):
            ops.add_donation(self.db, None, self.now)

    def test_failed_acknowledgement_retry_does_not_overissue(self):
        self.unit('UNT-FIRST')
        self.unit('UNT-SECOND')
        self.request()
        original = self.db.donations.find_one_and_update
        def issued_but_ack_lost(*args, **kwargs):
            original(*args, **kwargs)
            raise AutoReconnect('simulated lost acknowledgement')
        with patch.object(type(self.db.donations), 'find_one_and_update', side_effect=issued_but_ack_lost):
            with self.assertRaises(AutoReconnect):
                ops.fulfill_request(self.db, 'REQ-ROBUST')
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-ROBUST'), [])
        self.assertEqual(self.db.donations.count_documents({'unit_status': 'Issued'}), 1)

    def test_competing_requests_do_not_share_units(self):
        for i in range(7):
            self.unit(f'UNT-{i:03d}')
        for i in range(10):
            self.request(f'REQ-{i:03d}', 2)
        with ThreadPoolExecutor(max_workers=10) as pool:
            issued = list(pool.map(lambda i: ops.fulfill_request(self.db, f'REQ-{i:03d}'), range(10)))
        flattened = [unit for batch in issued for unit in batch]
        self.assertEqual(len(flattened), 7)
        self.assertEqual(len(set(flattened)), 7)
        self.assertTrue(all(row['units_fulfilled'] <= 2 for row in self.db.blood_requests.find({})))

    def test_fefo_equal_expiry_uses_unit_id(self):
        self.unit('UNT-ZZZ')
        self.unit('UNT-AAA')
        self.request()
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-ROBUST'), ['UNT-AAA'])

    def test_pagination_is_disjoint(self):
        for i in range(105):
            ops.add_donor(self.db, 'Synthetic Paging Donor', 25, 'Other', 'A+', 'Pune',
                          '0000000001', self.now, f'DNR-PAGE-{i:03d}')
        total, first = ops.list_donors(self.db, limit=100)
        _, second = ops.list_donors(self.db, skip=100, limit=100)
        self.assertEqual(total, 106)
        self.assertEqual(len(first), 100)
        self.assertEqual(len(second), 6)
        self.assertFalse({r['donor_id'] for r in first} & {r['donor_id'] for r in second})

    def test_seed_refuses_existing_data_and_cancel_preserves_it(self):
        env = {**os.environ, 'MONGODB_DB': self.name}
        for args, answer in [([], None), (['--reset'], 'cancel\n')]:
            result = subprocess.run([sys.executable, 'seed_data.py', '--records', '20', *args],
                                    env=env, input=answer, text=True, capture_output=True, timeout=30)
            self.assertEqual(result.returncode, 0 if args else 1)
            self.assertEqual(self.db.donors.count_documents({}), 1)
            self.assertEqual(self.db.donations.count_documents({}), 0)

    def test_streamlit_connection_error_is_friendly(self):
        import streamlit as st
        from streamlit.testing.v1 import AppTest
        st.cache_resource.clear()
        with patch('database.connect', side_effect=ConnectionError('Cannot connect to MongoDB.')):
            app = AppTest.from_file('app.py', default_timeout=30).run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(len(app.error), 1)
        st.cache_resource.clear()

    def test_auditor_reports_manual_corruption_without_repairing(self):
        unit = self.unit()
        self.request()
        self.db.donations.update_one({'_id': unit['_id']}, {'$set': {'donor_id': 'DNR-MISSING'}})
        self.db.blood_requests.update_one({}, {'$set': {'units_fulfilled': 99}})
        result = audit_database(self.db)
        self.assertFalse(result['ok'])
        self.assertGreaterEqual(result['error_count'], 2)
        self.assertEqual(self.db.blood_requests.find_one({})['units_fulfilled'], 99)

    def test_invalid_issue_limit_changes_nothing(self):
        self.unit()
        self.request()
        for value in (0, -1, True, '2', 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ops.fulfill_request(self.db, 'REQ-ROBUST', value)
        self.assertEqual(self.db.donations.count_documents({'unit_status': 'Issued'}), 0)

    def test_auditor_tolerates_invalid_reference_type(self):
        self.request()
        self.db.blood_requests.update_one({}, {'$set': {'request_id': ['not-a-string']}})
        self.assertFalse(audit_database(self.db)['ok'])

    def test_confirmed_reset_only_replaces_named_collections(self):
        self.db.audit_sentinel.insert_one({'keep': True})
        env = {**os.environ, 'MONGODB_DB': self.name}
        result = subprocess.run([sys.executable, 'seed_data.py', '--records', '20', '--reset'],
                                env=env, input=f'RESET {self.name}\n', text=True,
                                capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.db.donations.count_documents({}), 20)
        self.assertEqual(self.db.donors.count_documents({}), 10)
        self.assertEqual(self.db.blood_requests.count_documents({}), 4)
        self.assertEqual(self.db.audit_sentinel.count_documents({}), 1)

    def test_date_and_group_filters_have_exact_scope(self):
        self.unit('UNT-EARLIER')
        ops.add_donation(self.db, 'DNR-ROBUST', self.now.date(), 'UNT-TODAY')
        ops.add_donor(self.db, 'Synthetic Other Group', 25, 'Other', 'A+', 'Delhi',
                      '0000000002', self.now - timedelta(days=400), 'DNR-OTHER')
        ops.add_donation(self.db, 'DNR-OTHER', self.now.date(), 'UNT-OTHER')
        result = analytics.dashboard(self.db, 'O+', start=self.now.date(), end=self.now.date())
        self.assertEqual(result['available_by_group'], [{'blood_group': 'O+', 'count': 2}])
        self.assertEqual(sum(row['count'] for row in result['monthly_donations']), 1)
        self.assertEqual(result['kpis']['Total Donations'], 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
```

## verify_project.py

```python
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

from bson import ObjectId
from pymongo.errors import PyMongoError

import analytics
import operations as ops
from config import BLOOD_GROUPS, GENDERS, NEAR_EXPIRY_DAYS, utcnow
from database import connect, create_indexes
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
        require(isinstance(row[key], datetime), f'{key} is not a BSON date')
        return ops.as_utc(row[key])
    def identifier(row, key, prefix):
        require(isinstance(row['_id'], ObjectId), '_id is not an ObjectId')
        require(ops.readable_id(row[key], prefix, generate=False) == row[key], f'invalid {key}')
    donors = list(db.donors.find({}))
    requests = list(db.blood_requests.find({}))
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
    for row in db.donations.find({}):
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
    for collection, fields in [('donors', ['donor_id']), ('donations', ['donation_id', 'unit_id']),
                               ('blood_requests', ['request_id'])]:
        indexes = list(db[collection].index_information().values())
        for field in fields:
            if not any(index.get('unique') and index['key'] == [(field, 1)] for index in indexes):
                problem(f'Missing unique index: {collection}.{field}')
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
    name = 'blood_bank_test_' + uuid4().hex
    client, db = connect(name)
    results = []
    try:
        for size in sizes:
            for collection in ('donors', 'donations', 'blood_requests'):
                db.drop_collection(collection)
            create_indexes(db)
            start = perf_counter()
            dataset = generate_dataset(size)
            generated = perf_counter()
            insert_dataset(db, dataset)
            inserted = perf_counter()
            audit = audit_database(db)
            checked = perf_counter()
            start_dashboard = perf_counter()
            analytics.dashboard(db)
            dashboard_seconds = perf_counter() - start_dashboard
            explain = analytics.explain_inventory(db)
            results.append({'donation_size': size, 'generation_seconds': round(generated - start, 3),
                            'insertion_seconds': round(inserted - generated, 3),
                            'audit_seconds': round(checked - inserted, 3),
                            'dashboard_seconds': round(dashboard_seconds, 3), 'audit': audit,
                            'explain': {key: value['statistics'] for key, value in explain.items()}})
            print(f'Checked {size:,} donations: {"PASS" if audit["ok"] else "FAIL"}', flush=True)
    finally:
        assert db.name == name and name.startswith('blood_bank_test_')
        client.drop_database(name)
        client.close()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scale', action='store_true', help='Write only to a temporary test database and clean it up.')
    parser.add_argument('--sizes', type=int, nargs='+', default=[1000, 5000, 10000, 25000])
    args = parser.parse_args()
    if args.scale:
        if any(not 20 <= size <= 100000 for size in args.sizes):
            parser.error('Scale-check sizes must be between 20 and 100,000 donations.')
        result = scale_check(args.sizes)
        ok = all(row['audit']['ok'] for row in result)
    else:
        client, db = connect()
        try:
            result = audit_database(db)
            ok = result['ok']
        finally:
            client.close()
    print(json.dumps(result, indent=2))
    return 0 if ok else 1


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ConnectionError, PyMongoError):
        raise SystemExit('Verification could not finish. Check MongoDB and configuration; no credentials are displayed.') from None
```

## requirements.txt

```text
pymongo>=4.10,<5
streamlit>=1.52,<2
pandas>=2.2,<3
plotly>=5.24,<7
Faker>=33,<41
python-dotenv>=1.0,<2
```

## .env.example

```dotenv
MONGODB_URI=mongodb://localhost:27017/
MONGODB_DB=blood_bank_analytics
SHELF_LIFE_DAYS=42
LOW_STOCK_THRESHOLD=5
NEAR_EXPIRY_DAYS=7
```

## .gitignore

```text
.venv/
__pycache__/
*.pyc
.env
.env.*
!.env.example
.venv-*/
.pytest_cache/
*.log
```
