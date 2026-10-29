# Complete source code — Spark analytics dashboard

These labeled blocks match the executable files. The included official CSV and browser screenshots are separate artifacts. MongoDB-era source remains in the local backup; migrate_mongodb.py is optional. Playwright is used only by qa_visual.py for development browser checks.

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


PROJECT_DIR = Path(__file__).resolve().parent
SPARK_DATA_DIR = Path(os.getenv('SPARK_DATA_DIR', str(PROJECT_DIR / 'data' / 'spark'))).resolve()
SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[2]')
PUBLIC_DATA_DB = 'public'
MANAGEMENT_DB = 'management'
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
"""Local Spark SQL + immutable Parquet snapshots. No MongoDB runtime dependency.

PyArrow writes standard Parquet (no unofficial Windows Hadoop binaries).
Spark executes SQL analytics. The manifest is a LOCAL snapshot protocol, not
a distributed transaction service. Keep all files on a local disk.
"""
import json
import logging
import copy
from collections import OrderedDict
import os
import re
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4
import pyarrow as pa
import pyarrow.parquet as pq
from config import PROJECT_DIR, SPARK_DATA_DIR, SPARK_MASTER

class StorageError(RuntimeError):
    """Safe-to-display storage failure."""

TEXT, INT, DATE = pa.string(), pa.int64(), pa.timestamp('us', tz='UTC')
SCHEMAS = {
    'donors': pa.schema([('_id', TEXT), ('donor_id', TEXT), ('name', TEXT), ('age', INT),
        ('gender', TEXT), ('blood_group', TEXT), ('city', TEXT), ('contact_number', TEXT), ('registration_date', DATE)]),
    'donations': pa.schema([('_id', TEXT), ('donation_id', TEXT), ('unit_id', TEXT),
        ('donor_id', TEXT), ('blood_group', TEXT), ('donation_date', DATE), ('expiration_date', DATE),
        ('unit_status', TEXT), ('request_id', TEXT), ('issued_at', DATE)]),
    'blood_requests': pa.schema([('_id', TEXT), ('request_id', TEXT), ('hospital_name', TEXT),
        ('blood_group', TEXT), ('units_requested', INT), ('units_fulfilled', INT),
        ('request_date', DATE), ('request_status', TEXT)]),
    'daily_donations': pa.schema([('date', DATE), ('state', TEXT), ('blood_type', TEXT),
        ('donations', INT), ('source_row', INT), ('source_sha256', TEXT)]),
}
_registry_lock = threading.RLock()
_locks = {}
_spark = None
_query_cache = OrderedDict()

def sql_literal(value):
    return "'" + str(value).replace('\\', '\\\\').replace("'", "\\'") + "'"

def spark_session():
    global _spark
    with _registry_lock:
        if _spark is not None:
            return _spark
        java = os.getenv('SPARK_JAVA_HOME')
        if not java:
            candidates = list((PROJECT_DIR / '.runtime' / 'java21').glob('*/bin/java.exe'))
            if candidates:
                java = str(candidates[0].parent.parent)
        if java:
            os.environ['JAVA_HOME'] = java
        os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        from pyspark.sql import SparkSession
        try:
            _spark = (SparkSession.builder.master(SPARK_MASTER).appName('Blood Bank Spark Analytics')
                .config('spark.sql.session.timeZone', 'UTC')
                .config('spark.sql.shuffle.partitions', '4')
                .config('spark.default.parallelism', '2')
                .config('spark.sql.adaptive.enabled', 'true')
                .config('spark.sql.parser.escapedStringLiterals', 'false')
                .config('spark.ui.enabled', 'false')
                .config('spark.driver.bindAddress', '127.0.0.1')
                .config('spark.driver.host', '127.0.0.1').getOrCreate())
            _spark.sparkContext.setLogLevel('ERROR')
            return _spark
        except Exception:
            raise StorageError('Spark could not start. Install Java 21, set SPARK_JAVA_HOME in .env, '
                'install requirements.txt, and restart the application.') from None

class Store:
    """Writes hold locked() across read/validate/publish; readers pin one manifest."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.name = self.root.name
        with _registry_lock:
            self._lock = _locks.setdefault(str(self.root), threading.RLock())

    @contextmanager
    def locked(self):
        with self._lock:
            with (self.root / '.write.lock').open('a+b') as handle:
                handle.seek(0, 2)
                if handle.tell() == 0:
                    handle.write(b'0')
                    handle.flush()
                deadline = time.monotonic() + 10
                while True:
                    try:
                        handle.seek(0)
                        if os.name == 'nt':
                            import msvcrt
                            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                        else:
                            import fcntl
                            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise StorageError('Another writer is busy. Wait and retry; no change was published.') from None
                        time.sleep(.05)
                try:
                    yield
                finally:
                    handle.seek(0)
                    if os.name == 'nt':
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def manifest(self):
        path = self.root / 'current.json'
        if not path.exists():
            return {'version': None, 'tables': {}, 'metadata': {}}
        try:
            value = json.loads(path.read_text(encoding='utf-8'))
            if not isinstance(value['tables'], dict) or not isinstance(value['metadata'], dict):
                raise ValueError()
            for name, relative in value['tables'].items():
                if name not in SCHEMAS or not isinstance(relative, str):
                    raise ValueError()
                target = (self.root / relative).resolve()
                if not target.is_relative_to(self.root / 'versions') or not target.is_file():
                    raise ValueError()
            return value
        except (ValueError, KeyError, TypeError, OSError):
            raise StorageError('Snapshot metadata is invalid or a Parquet file is missing. '
                'Restore a previous snapshot; do not silently reseed.') from None

    def read_table(self, name, snapshot=None):
        if name not in SCHEMAS:
            raise ValueError('Unknown table.')
        snapshot = self.manifest() if snapshot is None else snapshot
        relative = snapshot['tables'].get(name)
        if relative is None:
            return pa.Table.from_pylist([], schema=SCHEMAS[name])
        try:
            return pq.read_table(self.root / relative, schema=SCHEMAS[name])
        except Exception:
            raise StorageError('Unable to read Parquet. Verify or restore the data files.') from None

    def rows(self, name, snapshot=None):
        return self.read_table(name, snapshot).to_pylist()

    def count(self, name, snapshot=None):
        snapshot = self.manifest() if snapshot is None else snapshot
        path = snapshot['tables'].get(name)
        try:
            return pq.ParquetFile(self.root / path).metadata.num_rows if path else 0
        except (OSError, pa.ArrowException):
            raise StorageError('Unable to read Parquet metadata. Verify or restore the snapshot.') from None

    def publish(self, tables, metadata=None):
        """Caller holds locked(). Publish all changed tables in ONE pointer swap."""
        current = self.manifest()
        revision = uuid4().hex
        folder = self.root / 'versions' / revision
        folder.mkdir(parents=True)
        paths = dict(current['tables'])
        try:
            for name, rows in tables.items():
                if name not in SCHEMAS:
                    raise ValueError('Unknown table.')
                table = rows.cast(SCHEMAS[name]) if isinstance(rows, pa.Table) else pa.Table.from_pylist(rows, schema=SCHEMAS[name])
                target = folder / (name + '.parquet')
                pq.write_table(table, target, compression='snappy', row_group_size=100000)
                # Windows FlushFileBuffers requires a writable file handle.
                with target.open('r+b') as saved:
                    os.fsync(saved.fileno())
                paths[name] = target.relative_to(self.root).as_posix()
            value = {'version': revision, 'tables': paths,
                     'metadata': current['metadata'] if metadata is None else metadata}
            with (folder / 'manifest.json').open('w', encoding='utf-8') as output:
                json.dump(value, output, indent=2, default=str)
                output.flush()
                os.fsync(output.fileno())
            pointer = self.root / ('pointer-' + revision + '.tmp')
            with pointer.open('w', encoding='utf-8') as output:
                json.dump(value, output, indent=2, default=str)
                output.flush()
                os.fsync(output.fileno())
            os.replace(pointer, self.root / 'current.json')
            return value
        except (OSError, pa.ArrowException):
            raise StorageError('Snapshot save could not be confirmed. Refresh and verify the committed snapshot '
                'before retrying; unpublished files may remain for inspection.') from None

    def query(self, sql, snapshot=None, explain=False):
        """Internal SQL only; all scan/plan/runtime failures stay UI-safe."""
        try:
            snapshot = self.manifest() if snapshot is None else snapshot
            files = tuple((name, path, (self.root / path).stat().st_mtime_ns,
                           (self.root / path).stat().st_size)
                          for name, path in sorted(snapshot['tables'].items()))
            key = (str(self.root), files, sql)
            if not explain:
                with _registry_lock:
                    if key in _query_cache:
                        _query_cache.move_to_end(key)
                        return copy.deepcopy(_query_cache[key])
            result = self._query(sql, snapshot, explain)
            if not explain:
                with _registry_lock:
                    _query_cache[key] = copy.deepcopy(result)
                    while len(_query_cache) > 192:
                        _query_cache.popitem(last=False)
            return result
        except StorageError:
            raise
        except Exception:
            logging.exception('Spark scan or query failed')
            raise StorageError('Spark query failed. Check the runtime and snapshot; inspect the terminal.') from None

    def _query(self, sql, snapshot=None, explain=False):
        """Temp views and immutable paths isolate each read."""
        snapshot = self.manifest() if snapshot is None else snapshot
        session = spark_session().newSession()
        names = set(re.findall(r'\b(?:FROM|JOIN)\s+(donors|donations|blood_requests|daily_donations)\b', sql, re.I))
        for name in names:
            relative = snapshot['tables'].get(name)
            if relative:
                frame = session.read.parquet((self.root / relative).as_uri())
            else:
                types = {TEXT: 'STRING', INT: 'BIGINT', DATE: 'TIMESTAMP'}
                columns = ', '.join(f'CAST(NULL AS {types[field.type]}) AS {field.name}' for field in SCHEMAS[name])
                frame = session.sql('SELECT ' + columns + ' WHERE false')
            frame.createOrReplaceTempView(name)
        try:
            if explain:
                return '\n'.join(row[0] for row in session.sql('EXPLAIN FORMATTED ' + sql).collect())
            return [row.asDict(recursive=True) for row in session.sql(sql).collect()]
        except Exception:
            raise StorageError('Spark query failed. Check the runtime and snapshot; inspect the terminal.') from None

    def stats(self):
        snapshot = self.manifest()
        return [{'table': name, 'rows': self.count(name, snapshot),
                 'parquet_bytes': (self.root / path).stat().st_size}
                for name, path in snapshot['tables'].items()]

def clear_query_cache():
    with _registry_lock:
        _query_cache.clear()

def connect(namespace='management'):
    if namespace not in ('management', 'public'):
        raise ValueError('Choose the management or public namespace.')
    return Store(SPARK_DATA_DIR / namespace)

if __name__ == '__main__':
    print('Spark:', spark_session().version)
    for namespace in ('management', 'public'):
        db = connect(namespace)
        print(namespace, db.stats())
```

## operations.py

```python
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
```

## analytics.py

```python
"""Spark SQL analytics over pinned Parquet snapshots."""
import argparse
import json
from datetime import timedelta
from time import perf_counter
from config import BLOOD_GROUPS, LOW_STOCK_THRESHOLD, NEAR_EXPIRY_DAYS, utcnow
from database import sql_literal
from operations import as_utc, available_sql, blood_group, integer, timestamp_sql, list_records

def history_filter(group=None, start=None, end=None, date_field='donation_date'):
    if date_field not in ('donation_date', 'request_date', 'registration_date'):
        raise ValueError('Invalid date field.')
    where = 'blood_group = ' + sql_literal(blood_group(group)) if group is not None else 'true'
    if (start is None) != (end is None):
        raise ValueError('Provide both a start and an end date.')
    if start is not None:
        start = as_utc(start).replace(hour=0, minute=0, second=0, microsecond=0)
        end = as_utc(end).replace(hour=0, minute=0, second=0, microsecond=0)
        if start > end:
            raise ValueError('Start date must not be after end date.')
        where += f' AND {date_field} >= {timestamp_sql(start)} AND {date_field} < {timestamp_sql(end + timedelta(days=1))}'
    return where

def grouped_sql(table, where, field, value='COUNT(*)', limit=None):
    sql = f'SELECT {field}, {value} AS count FROM {table} WHERE {where} GROUP BY {field} ORDER BY count DESC, {field}'
    return sql + (f' LIMIT {limit}' if limit else '')

def pipelines(now=None, group=None, start=None, end=None):
    now = now or utcnow()
    current = history_filter(group)
    history = history_filter(group, start, end)
    expiry = f"unit_status <> 'Issued' AND expiration_date <= {timestamp_sql(now)} AND {current}"
    near = available_sql(now, group) + f' AND expiration_date <= {timestamp_sql(now + timedelta(days=NEAR_EXPIRY_DAYS))}'
    def monthly(table, field, where, value):
        return (f"SELECT date_format({field}, 'yyyy-MM') AS month, {value} AS count FROM {table} "
                f"WHERE {where} GROUP BY date_format({field}, 'yyyy-MM') ORDER BY month")
    return {
        'available_by_group': grouped_sql('donations', available_sql(now, group), 'blood_group'),
        'monthly_donations': monthly('donations', 'donation_date', history, 'COUNT(*)'),
        'donation_distribution': grouped_sql('donations', history, 'blood_group'),
        'requested_by_group': grouped_sql('blood_requests', history_filter(group, start, end, 'request_date'), 'blood_group', 'SUM(units_requested)'),
        'monthly_requests': monthly('blood_requests', 'request_date', history_filter(group, start, end, 'request_date'), 'SUM(units_requested)'),
        'top_cities': grouped_sql('donors', history_filter(group, start, end, 'registration_date'), 'city', limit=10),
        'expired_count': f'SELECT COUNT(*) AS count FROM donations WHERE {expiry}',
        'near_expiry_count': f'SELECT COUNT(*) AS count FROM donations WHERE {near}',
        'inventory_status': f"""SELECT status, COUNT(*) AS count FROM (
            SELECT CASE WHEN unit_status = 'Issued' THEN 'Issued'
                        WHEN expiration_date <= {timestamp_sql(now)} THEN 'Expired'
                        ELSE unit_status END AS status FROM donations WHERE {current}
            ) GROUP BY status ORDER BY status""",
    }

def all_group_rows(rows, group=None):
    counts = {row['blood_group']: row['count'] for row in rows}
    groups = (blood_group(group),) if group is not None else BLOOD_GROUPS
    return [{'blood_group': item, 'count': counts.get(item, 0)} for item in groups]

def dashboard(db, group=None, start=None, end=None, now=None):
    now = now or utcnow()
    snapshot = db.manifest()
    result = {name: db.query(sql, snapshot) for name, sql in pipelines(now, group, start, end).items()}
    result['available_by_group'] = all_group_rows(result['available_by_group'], group)
    result['requested_by_group'] = all_group_rows(result['requested_by_group'], group)
    scope = history_filter(group)
    pending = db.query(f"""SELECT COUNT(*) AS count FROM blood_requests r
        LEFT JOIN (SELECT request_id, COUNT(*) AS actual FROM donations
                   WHERE unit_status = 'Issued' GROUP BY request_id) d
        ON r.request_id = d.request_id
        WHERE COALESCE(d.actual, 0) < r.units_requested AND {scope}""", snapshot)[0]['count']
    result['kpis'] = {
        'Total Registered Donors': db.query('SELECT COUNT(*) AS count FROM donors WHERE ' + scope, snapshot)[0]['count'],
        'Total Donations': db.query('SELECT COUNT(*) AS count FROM donations WHERE ' + scope, snapshot)[0]['count'],
        'Currently Available Blood Units': sum(r['count'] for r in result['available_by_group']),
        'Pending Blood Requests': pending,
        'Expired Blood Units': result['expired_count'][0]['count'],
        'Blood Units Near Expiration': result['near_expiry_count'][0]['count'],
    }
    result['low_stock'] = [r for r in result['available_by_group'] if r['count'] < LOW_STOCK_THRESHOLD]
    return result

def expiration_units(db, near=True, group=None, skip=0, limit=100, now=None):
    now = now or utcnow()
    where = (available_sql(now, group) + f' AND expiration_date <= {timestamp_sql(now + timedelta(days=NEAR_EXPIRY_DAYS))}'
             if near else f"unit_status <> 'Issued' AND expiration_date <= {timestamp_sql(now)} AND {history_filter(group)}")
    return list_records(db, 'donations', where, 'expiration_date, unit_id', skip, limit)

def explain_inventory(db, group='O+'):
    snapshot = db.manifest()
    sql = pipelines(group=group)['available_by_group']
    start = perf_counter()
    rows = db.query(sql, snapshot)
    return {'engine': 'Apache Spark SQL', 'seconds': round(perf_counter() - start, 4),
            'output_rows': len(rows), 'sql': sql, 'physical_plan': db.query(sql, snapshot, explain=True),
            'note': 'Actual physical plan and one elapsed observation; no database indexes or fixed speedup claimed.'}

if __name__ == '__main__':
    from database import connect
    parser = argparse.ArgumentParser()
    parser.add_argument('--explain', action='store_true', required=True)
    parser.add_argument('--group', choices=BLOOD_GROUPS, default='O+')
    args = parser.parse_args()
    print(json.dumps(explain_inventory(connect(), args.group), indent=2))
```

## public_data.py

```python
"""Download, validate, bulk-import and analyze with Spark SQL the official Malaysian dataset.

These are date/state/ABO aggregate observations, never individual blood units.
The operational demonstration database is not modified by this module.
"""
import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from config import PUBLIC_DATA_DB, utcnow
from database import connect, SCHEMAS, StorageError, sql_literal
from operations import as_utc, integer, timestamp_sql

SOURCE_URL = 'https://storage.data.gov.my/healthcare/blood_donations_state.csv'
CATALOG_URL = 'https://data.gov.my/data-catalogue/blood_donations_state'
LICENSE_URL = 'https://creativecommons.org/licenses/by/4.0/'
SOURCE_TITLE = 'Daily Blood Donations by Blood Group & State'
ATTRIBUTION = 'National Blood Centre / Ministry of Health Malaysia, via data.gov.my (CC BY 4.0)'
DATA_DIR = Path(__file__).parent / 'data'
CSV_PATH = DATA_DIR / 'raw' / 'blood_donations_state.csv'
MANIFEST_PATH = DATA_DIR / 'dataset_manifest.json'
COLLECTION = 'daily_donations'
GROUPS = ('a', 'b', 'ab', 'o')
STATES = ('Johor', 'Kedah', 'Kelantan', 'Melaka', 'Negeri Sembilan', 'Pahang', 'Perak',
          'Pulau Pinang', 'Sabah', 'Sarawak', 'Selangor', 'Terengganu', 'W.P. Kuala Lumpur')


def download_dataset(path=CSV_PATH):
    """Download only when missing; preserve existing raw snapshots byte-for-byte."""
    path = Path(path)
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid4().hex + '.part')
    try:
        request = Request(SOURCE_URL, headers={'User-Agent': 'BloodBankAnalytics-Educational/1.0'})
        with urlopen(request, timeout=60) as response, temporary.open('wb') as output:
            size = 0
            while chunk := response.read(1024 * 1024):
                size += len(chunk)
                if size > 64 * 1024 * 1024:
                    raise ValueError('Download exceeds the 64 MiB safety limit; review the source manually.')
                output.write(chunk)
        load_csv(temporary)  # Reject changed schema/HTML/error payload before accepting it.
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def load_csv(path=CSV_PATH):
    """Strict parsing; never silently remove records or invent missing categories."""
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    if list(frame.columns) != ['date', 'state', 'blood_type', 'donations'] or frame.empty:
        raise ValueError('Expected a nonempty CSV with date,state,blood_type,donations columns.')
    if not frame['date'].str.fullmatch(r'\d{4}-\d{2}-\d{2}').all():
        raise ValueError('Source dates must use YYYY-MM-DD.')
    frame['date'] = pd.to_datetime(frame['date'], format='%Y-%m-%d', utc=True, errors='raise')
    if (frame['date'] > pd.Timestamp(utcnow())).any():
        raise ValueError('Source contains future dates; inspect before importing.')
    if not frame['state'].isin(STATES).all() or not frame['blood_type'].isin((*GROUPS, 'all')).all():
        raise ValueError('Unexpected state/blood type. Review the source schema before importing.')
    if not frame['donations'].str.fullmatch(r'\d{1,9}').all():
        raise ValueError('Donation counts must be nonnegative integers of at most nine digits.')
    frame['donations'] = frame['donations'].astype('int64')
    if frame.duplicated(['date', 'state', 'blood_type']).any():
        raise ValueError('Duplicate date/state/blood-type source key.')
    if not frame.groupby(['date', 'state'])['blood_type'].nunique().eq(5).all():
        raise ValueError('Each date/state must contain all, a, b, ab and o observations.')
    return frame


def describe_csv(frame, path=CSV_PATH):
    path = Path(path)
    wide = frame.pivot(index=['date', 'state'], columns='blood_type', values='donations')
    difference = wide['all'] - wide[list(GROUPS)].sum(axis=1)
    return {
        'title': SOURCE_TITLE, 'source_url': SOURCE_URL, 'catalog_url': CATALOG_URL,
        'attribution': ATTRIBUTION, 'license': 'CC BY 4.0', 'license_url': LICENSE_URL,
        'snapshot_saved_at_utc': datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'file_bytes': path.stat().st_size,
        'rows': len(frame), 'date_state_observations': len(wide),
        'date_min': frame['date'].min().date().isoformat(), 'date_max': frame['date'].max().date().isoformat(),
        'states': sorted(frame['state'].unique().tolist()), 'days': int(frame['date'].nunique()),
        'published_total_donations': int(wide['all'].sum()),
        'abo_breakdown_total': int(wide[list(GROUPS)].sum().sum()),
        'published_total_minus_abo': int(difference.sum()),
        'date_state_rows_with_difference': int(difference.ne(0).sum()),
        'negative_difference_rows': int(difference.lt(0).sum()),
        'transformations': 'Raw CSV unchanged. Parquet dates parsed as UTC midnight and counts as integers; added source_row and source_sha256.',
        'not_provided': ['donor identities', 'Rh factor', 'individual unit IDs', 'hospital requests',
                         'unit expiration dates', 'current available inventory'],
    }



def arrow_table(frame, manifest):
    copy = frame.copy()
    copy['source_row'] = range(1, len(copy) + 1)
    copy['source_sha256'] = manifest['sha256']
    return pa.Table.from_pandas(copy, schema=SCHEMAS[COLLECTION], preserve_index=False)

def import_snapshot(db, frame, manifest, batch_size=1000, replace=False):
    integer(batch_size, 'Batch size', 1, 10000)
    table = arrow_table(frame, manifest)
    with db.locked():
        existing = db.manifest()
        if COLLECTION in existing['tables']:
            if existing['metadata'].get('sha256') == manifest['sha256']:
                # Verify content, not only a stored hash label + count.
                verify_snapshot(db, frame, manifest, existing)
                return {'imported': 0, 'already_present': True, 'rows': len(frame)}
            if not replace:
                raise ValueError('A different snapshot exists. Use --replace and confirm.')
        db.publish({COLLECTION: table}, metadata=manifest)
    return {'imported': len(frame), 'already_present': False, 'rows': len(frame)}

def verify_snapshot(db, frame, manifest, snapshot=None):
    actual = db.read_table(COLLECTION, snapshot)
    expected = arrow_table(frame, manifest)
    if actual.num_rows != expected.num_rows:
        raise ValueError('Stored record count differs from the CSV.')
    actual = actual.take(pc.sort_indices(actual, sort_keys=[('source_row', 'ascending')]))
    if not actual.equals(expected, check_metadata=False):
        raise ValueError('Source mismatch: stored values differ from the CSV.')
    return {'verified_rows': len(frame), 'sha256': manifest['sha256'], 'ok': True}

def public_filter(start=None, end=None, state=None, group='all'):
    if group not in (*GROUPS, 'all'):
        raise ValueError('Choose All, A, B, AB or O; Rh types are not available.')
    if state is not None and state not in STATES:
        raise ValueError('Select a published state.')
    query = 'blood_type = ' + sql_literal(group)
    if state:
        query += ' AND state = ' + sql_literal(state)
    if (start is None) != (end is None):
        raise ValueError('Select both start and end dates.')
    if start is not None:
        start = as_utc(start).replace(hour=0, minute=0, second=0, microsecond=0)
        end = as_utc(end).replace(hour=0, minute=0, second=0, microsecond=0)
        if start > end:
            raise ValueError('Start must not follow end.')
        query += f' AND date >= {timestamp_sql(start)} AND date < {timestamp_sql(end + timedelta(days=1))}'
    return query

def public_pipelines(start=None, end=None, state=None, group='all'):
    where = public_filter(start, end, state, group)
    def grouped(expression, field):
        return (f'SELECT {expression} AS {field}, SUM(donations) AS count FROM daily_donations '
                f'WHERE {where} GROUP BY {expression} ORDER BY {field}')
    breakdown = where.replace("blood_type = 'all'", "blood_type IN ('a','b','ab','o')", 1) if group == 'all' else where
    return {
        'monthly': grouped("date_format(date, 'yyyy-MM')", 'month'),
        'yearly': grouped("date_format(date, 'yyyy')", 'year'),
        'daily': grouped("date_format(date, 'yyyy-MM-dd')", 'day'),
        'states': f'SELECT state, SUM(donations) AS count FROM daily_donations WHERE {where} GROUP BY state ORDER BY count DESC, state LIMIT 13',
        'groups': f'SELECT upper(blood_type) AS blood_group, SUM(donations) AS count FROM daily_donations WHERE {breakdown} GROUP BY blood_type ORDER BY blood_type',
        'weekdays': grouped('((dayofweek(date) + 5) % 7) + 1', 'weekday'),
    }

def public_report(db, start=None, end=None, state=None, group='all'):
    snapshot = db.manifest()
    output = {name: db.query(sql, snapshot) for name, sql in public_pipelines(start, end, state, group).items()}
    output['total'] = sum(row['count'] for row in output['monthly'])
    output['days'] = len(output['daily'])
    output['states_count'] = len(output['states'])
    output['rows'] = db.query('SELECT COUNT(*) AS count FROM daily_donations WHERE ' + public_filter(start, end, state, group), snapshot)[0]['count']
    output['abo_total'] = sum(row['count'] for row in output['groups'])
    output['total_minus_abo'] = output['total'] - output['abo_total'] if group == 'all' else None
    return output

def explain_public(db, start=None, end=None, state=None, group='all'):
    from time import perf_counter
    sql = public_pipelines(start, end, state, group)['monthly']
    snapshot = db.manifest()
    before = perf_counter()
    rows = db.query(sql, snapshot)
    elapsed = perf_counter() - before
    return {'engine': 'Apache Spark SQL / Parquet', 'seconds': round(elapsed, 4),
            'output_rows': len(rows), 'sql': sql, 'physical_plan': db.query(sql, snapshot, explain=True),
            'note': 'One observed run, not a benchmark. Inspect FileScan, PushedFilters and Exchange; no MongoDB indexes exist.'}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', type=Path, default=CSV_PATH)
    parser.add_argument('--batch-size', type=int, default=1000, help='Accepted for CLI compatibility; Parquet uses 100000-row groups.')
    parser.add_argument('--verify-only', action='store_true')
    parser.add_argument('--replace', action='store_true')
    args = parser.parse_args()
    integer(args.batch_size, 'Batch size', 1, 10000)
    if not args.csv.exists():
        if args.csv != CSV_PATH or args.verify_only:
            raise ValueError('CSV not found. Run python public_data.py to download the official dataset.')
        download_dataset(args.csv)
    frame = load_csv(args.csv)
    manifest = describe_csv(frame, args.csv)
    db = connect('public')
    if args.verify_only:
        print(json.dumps(verify_snapshot(db, frame, manifest), indent=2))
        return
    if args.replace and input('Type exactly REPLACE public.daily_donations: ').strip() != 'REPLACE public.daily_donations':
        print('Cancelled. No data changed.')
        return
    result = import_snapshot(db, frame, manifest, args.batch_size, args.replace)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({**result, 'storage': str(db.root), 'source_total': manifest['published_total_donations']}, indent=2))

if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, StorageError, EOFError) as exc:
        raise SystemExit(str(exc)) from None
```

## presentation.py

```python
"""Readable charts and descriptive findings from actual Spark report results."""
from calendar import monthrange
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

RED = '#B42335'
TEAL = '#147D73'
INK = '#253247'
MUTED = '#758196'
COLORS = {'A': RED, 'B': TEAL, 'AB': '#B88622', 'O': '#5578B1'}


def polish(fig, height=340, ylabel='Recorded donations'):
    fig.update_layout(template='plotly_white', height=height,
        font=dict(family='Arial, sans-serif', size=12, color=INK),
        paper_bgcolor='white', plot_bgcolor='white',
        margin=dict(l=12, r=18, t=24, b=12),
        hoverlabel=dict(bgcolor='white'),
        legend=dict(orientation='h', yanchor='bottom', y=1.01, x=0),
        xaxis_title=None, yaxis_title=ylabel)
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor='#EDF0F5', zeroline=False, tickformat=',~s')
    return fig


def monthly_figure(rows, start=None, end=None):
    frame = pd.DataFrame(rows)
    fig = px.area(frame, x='month', y='count', color_discrete_sequence=[RED])
    fig.update_traces(line=dict(width=2.5), fillcolor='rgba(180,35,53,0.08)',
                      hovertemplate='%{x}<br><b>%{y:,.0f}</b> recorded donations<extra></extra>')
    fig.update_layout(hovermode='x unified')
    if start and end:
        partial = []
        for row in rows:
            year, month = map(int, row['month'].split('-'))
            first = datetime(year, month, 1).date()
            last = datetime(year, month, monthrange(year, month)[1]).date()
            if first < start or last > end:
                partial.append(row)
        if partial:
            fig.add_scatter(x=[row['month'] for row in partial], y=[row['count'] for row in partial],
                mode='markers', name='Partial calendar month',
                marker=dict(color='#B88622', size=10, line=dict(color='white', width=1)),
                hovertemplate='%{x} (partial month)<br>%{y:,.0f} donations<extra></extra>')
    return polish(fig, 370)


def group_figure(rows):
    frame = pd.DataFrame(rows)
    fig = px.pie(frame, names='blood_group', values='count', color='blood_group',
                 color_discrete_map=COLORS, hole=.67)
    fig.update_traces(textposition='outside', textinfo='label+percent', sort=False,
                      hovertemplate='Group %{label}<br>%{value:,.0f} donations<br>%{percent}<extra></extra>')
    fig.add_annotation(text='ABO<br>mix', x=.5, y=.5, showarrow=False,
                       font=dict(size=18, color=INK))
    return polish(fig, 370)


def state_figure(rows):
    frame = pd.DataFrame(rows).sort_values('count')
    fig = px.bar(frame, x='count', y='state', orientation='h', text='count',
                 color_discrete_sequence=[TEAL])
    fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside', cliponaxis=False,
                      hovertemplate='%{y}<br><b>%{x:,.0f}</b> donations<extra></extra>')
    fig.update_layout(showlegend=False, xaxis_title='Recorded donations', yaxis_title=None)
    fig = polish(fig, max(260, 32 * len(rows) + 60), ylabel=None)
    fig.update_xaxes(gridcolor='#EDF0F5', showgrid=True, tickformat=',~s')
    fig.update_yaxes(showgrid=False, automargin=True)
    return fig


def daily_figure(rows):
    frame = pd.DataFrame(rows)
    frame['day'] = pd.to_datetime(frame['day'])
    frame = frame.set_index('day').asfreq('D')
    fig = go.Figure()
    fig.add_scatter(x=frame.index, y=frame['count'], name='Daily count',
                    mode='lines', line=dict(color='#CBD2DE', width=1), connectgaps=False)
    rolling = frame['count'].rolling(7, min_periods=7).mean()
    fig.add_scatter(x=frame.index, y=rolling, name='7-day average', mode='lines',
                    line=dict(color=RED, width=2), connectgaps=False)
    fig.update_layout(hovermode='x unified')
    return polish(fig, 360)


def yearly_figure(rows, start=None, end=None):
    frame = pd.DataFrame(rows)
    fig = px.bar(frame, x='year', y='count', color_discrete_sequence=[TEAL])
    fig.update_traces(hovertemplate='%{x}<br>%{y:,.0f} donations<extra></extra>')
    if start and end:
        colors = ['#B88622' if datetime(int(row['year']), 1, 1).date() < start
                  or datetime(int(row['year']), 12, 31).date() > end else TEAL for row in rows]
        fig.update_traces(marker_color=colors)
    return polish(fig)


def weekday_figure(rows, daily, average=True):
    occurrences = {i: 0 for i in range(1, 8)}
    for row in daily:
        occurrences[datetime.strptime(row['day'], '%Y-%m-%d').isoweekday()] += 1
    totals = {row['weekday']: row['count'] for row in rows}
    names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    values = [(totals.get(i, 0) / occurrences[i] if occurrences[i] else None)
              if average else totals.get(i, 0) for i in range(1, 8)]
    fig = px.bar(x=names, y=values, color_discrete_sequence=[RED])
    fig.update_traces(hovertemplate='%{x}<br>%{y:,.1f} donations<extra></extra>')
    return polish(fig, ylabel='Average per observed day' if average else 'Recorded donations')


def findings(result):
    """No predictions: statements follow the current selection exactly."""
    items = []
    if result['states'] and result['total']:
        top = result['states'][0]
        items.append(('Leading collection state', top['state'],
                      f"{top['count']:,} donations · {top['count'] / result['total']:.1%} of selected totals"))
    if result['groups'] and result['abo_total']:
        top = max(result['groups'], key=lambda row: row['count'])
        items.append(('Largest recorded ABO group', top['blood_group'],
                      f"{top['count']:,} donations · {top['count'] / result['abo_total']:.1%} of the ABO breakdown"))
    if result['daily']:
        peak = max(result['daily'], key=lambda row: (row['count'], row['day']))
        items.append(('Highest recorded day', peak['day'], f"{peak['count']:,} donations in the current selection"))
    return items


def complete_month_change(monthly, start, end):
    """Compare only fully included calendar months with a nonzero prior year."""
    values = {row['month']: row['count'] for row in monthly}
    for month in sorted(values, reverse=True):
        year, number = map(int, month.split('-'))
        first = datetime(year, number, 1).date()
        last = datetime(year, number, monthrange(year, number)[1]).date()
        previous = f'{year - 1}-{number:02d}'
        prior_first = datetime(year - 1, number, 1).date()
        if first >= start and last <= end and prior_first >= start and values.get(previous, 0) > 0:
            return month, (values[month] - values[previous]) / values[previous]
    return None
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

st.set_page_config(page_title='Blood Bank Analytics', page_icon='🩸', layout='wide')

try:
    import analytics
    import presentation as visuals
    import public_data as source
    import operations as ops
    from config import (BLOOD_GROUPS, GENDERS, LOW_STOCK_THRESHOLD,
                        NEAR_EXPIRY_DAYS, SHELF_LIFE_DAYS, PUBLIC_DATA_DB, utcnow)
    from database import connect, StorageError, clear_query_cache
except ValueError:
    st.error('Invalid configuration. Shelf life and near-expiry days must be integers from 1 to 3650; '
             'the low-stock threshold must be an integer from 1 to 1000000. Check .env and restart.')
    st.stop()


@st.cache_resource
def database_connection():
    db = connect()
    ops.reconcile_requests(db)
    return db


@st.cache_resource
def public_database_connection():
    return connect(PUBLIC_DATA_DB)


def table(rows):
    if not rows:
        st.info('No matching records for this selection.')
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
    except (StorageError, OSError):
        st.error('The snapshot operation could not complete. Refresh and inspect the current records before retrying. '
                 'Inventory and request summaries are published together.')


def chart(rows, x, title, kind='bar', ylabel='Units / records'):
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
    elif kind == 'daily':
        fig = px.line(frame, x=x, y='count')
    elif kind == 'pie':
        fig = px.pie(frame, names=x, values='count', hole=.35)
    else:
        fig = px.bar(frame, x=x, y='count', text_auto=True)
    fig = visuals.polish(fig, ylabel=ylabel)
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
        table(db.stats())
        st.caption('Immutable local Parquet tables; queries execute in Apache Spark SQL.')
        st.subheader('Spark SQL queries and actual outputs')
        for name, sql in analytics.pipelines(group=group, start=start, end=end).items():
            with st.expander(name.replace('_', ' ').title()):
                st.code(sql, language='sql')
                table(result[name])
        st.subheader('Spark execution plan')
        st.caption('Inspect Parquet FileScan, pushed filters and shuffle exchanges. No MongoDB indexes remain.')
        if st.button('Run explain comparison'):
            st.json(analytics.explain_inventory(db, group or 'O+'))



def donor_page(db):
    st.title('Donor Management')
    st.metric('Total registered donors', db.count('donors'))
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
    sql = analytics.pipelines(group=group)['available_by_group']
    table(analytics.all_group_rows(db.query(sql), group))
    status = st.selectbox('Unit status', ['All', 'Available', 'Issued', 'Expired'])
    total, _ = ops.list_units(db, group, status, limit=1)
    skip = offset(total, 'inventory_page')
    _, rows = ops.list_units(db, group, status, skip)
    table(rows)
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
    total, _ = ops.list_requests(db, group, status, limit=1)
    skip = offset(total, 'request_page')
    _, rows = ops.list_requests(db, group, status, skip)
    table(rows)



@st.cache_data(show_spinner=False, ttl=900, max_entries=32)
def cached_public_report(root, version, start, end, state, group, _db):
    return source.public_report(_db, start, end, state, group)


def public_report_page(db, detailed=False):
    st.caption('PUBLIC DATA  /  MALAYSIA  /  APACHE SPARK')
    st.title('Explore donation analytics' if detailed else 'Blood donation insights')
    st.write('Understand donation activity across time, collection states and ABO blood groups.')
    snapshot = db.manifest()
    metadata = snapshot['metadata']
    if not db.count(source.COLLECTION, snapshot):
        st.info('Import the included official CSV to display the real-data dashboard.')
        st.code('python public_data.py')
        return
    if metadata.get('date_min') and metadata.get('date_max'):
        first, last = date.fromisoformat(metadata['date_min']), date.fromisoformat(metadata['date_max'])
    else:
        bounds = db.query('SELECT MIN(date) AS first, MAX(date) AS last FROM daily_donations', snapshot)[0]
        first, last = bounds['first'].date(), bounds['last'].date()
    st.caption(f'Source: National Blood Centre / Ministry of Health Malaysia · '
               f'{db.count(source.COLLECTION, snapshot):,} source records · '
               f'{first:%d %b %Y} – {last:%d %b %Y} · CC BY 4.0')
    with st.container(border=True):
        dates, states, groups = st.columns([2, 1.3, 1])
        with dates:
            selected = st.date_input('Dataset date range (inclusive)', value=(first, last),
                                     min_value=first, max_value=last)
        with states:
            state_choice = st.selectbox('Collection state', ['All', *source.STATES])
        with groups:
            group_choice = st.selectbox('ABO blood group (Rh not supplied)', ['All', 'A', 'B', 'AB', 'O'])
    if len(selected) != 2:
        st.info('Select both dates.')
        return
    start, end = selected
    state = None if state_choice == 'All' else state_choice
    group = group_choice.lower()
    with st.spinner('Preparing your donation report with Spark SQL...'):
        result = cached_public_report(str(db.root), snapshot['version'], start, end, state, group, db)
    avg = result['total'] / result['days'] if result['days'] else 0
    metrics = [('Recorded donations', f"{result['total']:,}", 'Published recorded counts for your selection.'),
               ('Average / observed day', f'{avg:,.1f}', 'Total divided by calendar dates represented in the selection.'),
               ('Collection states', result['states_count'], 'Collection location; this is not donor residence.'),
               ('Days represented', f"{result['days']:,}", 'Distinct dates in the selected source observations.')]
    for column, (label, value, help_text) in zip(st.columns(4), metrics):
        with column:
            with st.container(border=True):
                st.metric(label, value, help=help_text)
    scope = f'{state_choice} states' if state is None else state
    st.caption(f'{scope} · {"All ABO groups" if group == "all" else group_choice + " group"} · '
               f'{start:%d %b %Y} – {end:%d %b %Y}. Counts describe donations, not unique donors or available stock.')
    if group == 'all' and result['total_minus_abo']:
        st.info(f"Source quality: published totals differ from the ABO breakdown by "
                f"{result['total_minus_abo']:,} donations in this selection. Both are preserved.")
    if not result['rows']:
        st.info('No observations match this selection. Adjust the date range, state or group.')
        return
    overview, patterns, evidence = st.tabs(['Overview', 'Trends & patterns', 'Source & data'])
    with overview:
        main_chart, mix = st.columns([2.1, 1])
        with main_chart:
            with st.container(border=True):
                st.subheader('Monthly donation trend')
                change = visuals.complete_month_change(result['monthly'], start, end)
                if change:
                    st.caption(f'{change[0]}: {change[1]:+.1%} versus the same complete month one year earlier.')
                st.caption('Gold markers identify incomplete boundary months; compare complete periods.')
                st.plotly_chart(visuals.monthly_figure(result['monthly'], start, end), width='stretch', key='public_monthly')
        with mix:
            with st.container(border=True):
                st.subheader('ABO distribution')
                st.caption(f"{result['abo_total']:,} categorized donations")
                st.plotly_chart(visuals.group_figure(result['groups']), width='stretch', key='public_abo')
        insights = visuals.findings(result)
        for column, (label, value, explanation) in zip(st.columns(3), insights):
            with column:
                with st.container(border=True):
                    st.caption(label.upper())
                    st.subheader(value)
                    st.caption(explanation)
        with st.container(border=True):
            st.subheader('Donation activity by collection state')
            st.caption('Ranked recorded totals. Coverage varies by collection site; these are not population rates.')
            st.plotly_chart(visuals.state_figure(result['states']), width='stretch', key='public_states')
    with patterns:
        with st.container(border=True):
            st.subheader('Daily activity and seven-day average')
            st.caption('The average requires seven consecutive observed calendar days; missing dates are not filled with zero.')
            st.plotly_chart(visuals.daily_figure(result['daily']), width='stretch', key='public_daily')
        annual, weekday = st.columns(2)
        with annual:
            with st.container(border=True):
                st.subheader('Yearly donation totals')
                st.caption('Gold bars identify incomplete boundary years. Compare equivalent date ranges.')
                st.plotly_chart(visuals.yearly_figure(result['yearly'], start, end), width='stretch', key='public_yearly')
        with weekday:
            with st.container(border=True):
                st.subheader('Weekday pattern')
                measure = st.radio('Weekday measure', ['Average per observed day', 'Total donations'],
                                   horizontal=True)
                st.plotly_chart(visuals.weekday_figure(result['weekdays'], result['daily'],
                    average=measure == 'Average per observed day'), width='stretch', key='public_weekdays')
                st.caption('Monday–Sunday. Averages account for how many observed dates fall on each weekday.')
    with evidence:
        st.subheader('A traceable public dataset')
        st.markdown(f'[{source.SOURCE_TITLE}]({source.CATALOG_URL}) · '
                    f'[CC BY 4.0 license]({source.LICENSE_URL})')
        st.write('Published by the National Blood Centre / Ministry of Health Malaysia through data.gov.my. '
                 'The original CSV is preserved. Spark SQL aggregates its recorded counts.')
        st.caption(f"{result['rows']:,} matching date/state/group observations · "
                   f"{result['abo_total']:,} ABO breakdown donations")
        st.write('Overall totals select published “all” subtotal rows. The ABO distribution selects only '
                 'A, B, AB and O; the two sets are never added together.')
        with st.expander('Coverage and interpretation'):
            st.write('The publisher covers selected BBISv2 sites, not every donation nationwide. '
                     'Collection state is not donor residence. Annual boundary periods can be incomplete.')
            st.write('Rh factor, individual donors, hospital requests, expiry and current stock are not supplied. '
                     'The management pages use separate synthetic demonstration data.')
            st.caption('State comparisons are descriptive, not population-adjusted donation rates or causal findings.')
        exports = {'Monthly donations': ('monthly', 'month'),
                   'State totals': ('states', 'state'), 'ABO distribution': ('groups', 'blood_group'),
                   'Daily donations': ('daily', 'day')}
        export_choice = st.selectbox('Export selected summary', list(exports))
        export_key, _ = exports[export_choice]
        export_frame = pd.DataFrame(result[export_key]).assign(
            selection_start=start.isoformat(), selection_end=end.isoformat(),
            selection_state=state_choice, selection_group=group_choice)
        st.download_button('Download summary CSV', export_frame.to_csv(index=False).encode('utf-8-sig'),
                           file_name=f'blood_donations_{export_key}.csv', mime='text/csv')
        st.caption('Export follows the filters above. Attribution: Ministry of Health Malaysia / data.gov.my, CC BY 4.0.')
        if detailed:
            st.subheader('Source observations')
            skip = offset(result['rows'], 'public_rows_page')
            query = source.public_filter(start, end, state, group)
            _, rows = ops.list_records(db, source.COLLECTION, query, 'date, state, blood_type', skip)
            table(rows)
            st.caption(f"Source SHA-256: {metadata.get('sha256', 'not recorded')}")
            for name, sql in source.public_pipelines(start, end, state, group).items():
                with st.expander(name.title() + ' · Spark SQL'):
                    st.code(sql, language='sql')
                    table(result[name])
            if st.button('Explain public dataset query'):
                explanation = source.explain_public(db, start, end, state, group)
                st.caption(f"Observed query time: {explanation['seconds']:.3f}s. This is one run, not a benchmark.")
                st.code(explanation['physical_plan'], language='text')
    st.caption('Educational descriptive analytics · Real public aggregates · Inventory workflows use synthetic data.')


def main():
    st.sidebar.title('Blood Bank Analytics')
    page = st.sidebar.radio('Navigation', ['Dashboard', 'Donor Management', 'Blood Inventory',
                                            'Blood Requests', 'Analytics'])
    mode = st.sidebar.selectbox('Analytics data source', ['Public dataset (real)', 'Inventory demo (simulated)'])
    st.sidebar.caption('Apache Spark SQL + local Parquet storage')
    if st.sidebar.button('Refresh data'):
        st.cache_data.clear()
        clear_query_cache()
    st.sidebar.caption('Real public analytics, with a separate inventory demonstration.')
    with st.sidebar.expander('About this project'):
        st.caption('Apache Spark SQL · Parquet · Streamlit. Educational use. One local application process.')
    try:
        if page in ('Dashboard', 'Analytics') and mode == 'Public dataset (real)':
            public_db = public_database_connection()
            public_report_page(public_db, detailed=page == 'Analytics')
            return
        st.info('SIMULATED management data. These donors, units and requests are not from the public dataset.')
        db = database_connection()
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
        st.code('python database.py')
    except (StorageError, OSError):
        st.error('Spark/storage operation failed. Check Java 21, the project data folder and terminal diagnostics, '
                 'then refresh. No credentials are displayed here.')


if __name__ == '__main__':
    main()
```

## seed_data.py

```python
"""Reproducible synthetic data and Parquet snapshot insertion.

--records N means N DONATIONS, plus ceil(N/2) donors and ceil(N/5) requests.
No real people, telephone numbers or hospital identities are used.
"""
import argparse
import heapq
import math
import random
from collections import defaultdict
from datetime import datetime, time, timedelta, timezone

from faker import Faker

from config import BLOOD_GROUPS, GENDERS, SHELF_LIFE_DAYS, utcnow
from database import connect, StorageError
from operations import as_utc, integer, validate_state

CITIES = ('Mumbai', 'Delhi', 'Pune', 'Bengaluru', 'Chennai', 'Kolkata', 'Jaipur', 'Hyderabad')


def generate_dataset(records=5000, seed=42, now=None):
    integer(records, 'Donation records', 20, 1000000)
    now = as_utc(now) if now is not None else datetime.combine(utcnow().date(), time.min, timezone.utc)
    rng = random.Random(seed)
    fake = Faker('en_IN')
    fake.seed_instance(seed)
    def oid():
        return f'{rng.getrandbits(128):032x}'
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


def insert_dataset(db, dataset, batch_size=1000, replace=False):
    integer(batch_size, 'Batch size', 1, 10000)
    validate_state(dataset)
    with db.locked():
        if not replace and any(db.count(name) for name in ('donors', 'donations', 'blood_requests')):
            raise ValueError('Storage is not empty. No records changed. Use --reset and confirm.')
        db.publish(dataset)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--records', type=int, default=5000)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--batch-size', type=int, default=1000, help='Compatibility option; Parquet uses row groups.')
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()
    db = connect()
    if args.reset:
        print('Target:', db.root, 'Only management Parquet tables are replaced; old snapshots remain.')
        if input('Type exactly RESET management: ').strip() != 'RESET management':
            print('Cancelled. No data changed.')
            return
    dataset = generate_dataset(args.records, args.seed)
    insert_dataset(db, dataset, args.batch_size, replace=args.reset)
    print({name: len(rows) for name, rows in dataset.items()})
    print('Run: python -m streamlit run app.py')

if __name__ == '__main__':
    try:
        main()
    except (ValueError, StorageError, OSError, EOFError) as exc:
        raise SystemExit(str(exc)) from None
```

## migrate_mongodb.py

```python
"""One-time READ-ONLY MongoDB export to management Parquet snapshots.

Stop the old app before running. Requires pymongo ONLY for this migration.
The active application, generator and analytics have no MongoDB dependency.
"""
import json
import os
from datetime import datetime, timezone
from database import connect, StorageError
from operations import TABLES, validate_state


def migrate():
    try:
        from pymongo import MongoClient
    except ImportError:
        raise SystemExit('Only for old-data migration: python -m pip install pymongo') from None
    target = connect()
    with target.locked():
        if any(target.count(name) for name in TABLES):
            raise ValueError('Management storage is not empty. Migration refuses to overwrite it.')
        client = MongoClient(os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'),
                             tz_aware=True, serverSelectionTimeoutMS=5000, timeoutMS=15000)
        try:
            source = client[os.getenv('MONGODB_DB', 'blood_bank_analytics')]
            data = {name: list(source[name].find({}).sort('_id', 1)) for name in TABLES}
            second = {name: list(source[name].find({}).sort('_id', 1)) for name in TABLES}
            if data != second:
                raise ValueError('Source changed while exporting. Stop the old app and retry.')
            for rows in data.values():
                for row in rows:
                    row['_id'] = str(row['_id'])
            validate_state(data)
            if not any(data.values()):
                raise ValueError('The old database is empty. Use seed_data.py if you want demo records.')
            target.publish(data, metadata={'origin': 'read-only MongoDB migration',
                                           'migrated_at_utc': datetime.now(timezone.utc).isoformat()})
            for name in TABLES:
                if target.rows(name) != data[name]:
                    raise ValueError('Migration verification mismatch: ' + name)
            return {'ok': True, 'source_unchanged': True,
                    'counts': {name: len(rows) for name, rows in data.items()}, 'target': str(target.root)}
        finally:
            client.close()


if __name__ == '__main__':
    try:
        print(json.dumps(migrate(), indent=2))
    except (ValueError, StorageError) as exc:
        raise SystemExit(str(exc)) from None
    except Exception:
        raise SystemExit('Migration could not finish. Check the old MongoDB service. No source data was modified.') from None
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
```

## qa_full_dataset.py

```python
"""Full-snapshot QA, not synthetic screenshots or invented benchmark results.

Uses actual public data and a temporary COPY of management records for UI checks.
Writes qa_results/full_dataset.json; leaves original operational data unchanged.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from unittest.mock import patch

import streamlit as st
from streamlit.testing.v1 import AppTest
from database import connect, Store, spark_session
import public_data as source
from verify_project import audit_database


def main():
    started = perf_counter()
    public, management = connect('public'), connect()
    frame = source.load_csv()
    manifest = source.describe_csv(frame)
    verification = source.verify_snapshot(public, frame, manifest)
    totals = source.public_report(public)
    assert totals['total'] == manifest['published_total_donations']
    assert totals['abo_total'] == manifest['abo_breakdown_total']
    assert totals['total_minus_abo'] == manifest['published_total_minus_abo']
    audit = audit_database(management)
    assert audit['ok'], audit
    pages = []
    with TemporaryDirectory(prefix='blood_bank_full_ui_test_') as folder:
        demo = Store(folder)
        saved = management.manifest()
        with demo.locked():
            demo.publish({name: management.read_table(name, saved)
                          for name in ('donors', 'donations', 'blood_requests')})
        def selected(namespace='management'):
            return public if namespace == 'public' else demo
        st.cache_resource.clear()
        try:
            with patch('database.connect', side_effect=selected):
                app = AppTest.from_file('app.py', default_timeout=180).run()
                assert not app.exception and not app.error
                assert app.metric[0].value == f"{manifest['published_total_donations']:,}"
                pages.append('Public Dashboard')
                app.sidebar.radio[0].set_value('Analytics').run()
                assert not app.exception and not app.error
                pages.append('Public Analytics')
                app.sidebar.selectbox[0].set_value('Inventory demo (simulated)').run()
                for page in ('Dashboard', 'Donor Management', 'Blood Inventory', 'Blood Requests', 'Analytics'):
                    app.sidebar.radio[0].set_value(page).run()
                    assert not app.exception and not app.error, page
                    pages.append('Simulated ' + page)
        finally:
            st.cache_resource.clear()
    result = {'checked_at_utc': datetime.now(timezone.utc).isoformat(),
              'spark_version': spark_session().version, 'ok': True,
              'source_verification': verification, 'operational_audit': audit,
              'public_totals': {key: totals[key] for key in
                                ('total', 'abo_total', 'total_minus_abo', 'rows', 'days', 'states_count')},
              'ui_pages_passed': pages,
              'explain': source.explain_public(public, group='o'),
              'seconds': round(perf_counter() - started, 3)}
    output = Path('qa_results')
    output.mkdir(exist_ok=True)
    (output / 'full_dataset.json').write_text(json.dumps(result, indent=2, default=str) + '\n', encoding='utf-8')
    print(json.dumps({key: value for key, value in result.items() if key != 'explain'}, indent=2))


if __name__ == '__main__':
    main()
```

## qa_visual.py

```python
"""Optional browser review; requires Playwright as a development tool only.

Start Streamlit on port 8501 first. Captures the real running application and
tests report tabs/filters without modifying donor, unit or request records.
"""
import json
from pathlib import Path
from time import perf_counter
from playwright.sync_api import sync_playwright


def main():
    output = Path('qa_results')
    output.mkdir(exist_ok=True)
    started = perf_counter()
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1440, 'height': 2100}, device_scale_factor=1)
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto('http://127.0.0.1:8501', wait_until='domcontentloaded')
        page.get_by_text('9,896,044', exact=True).wait_for(timeout=180000)
        page.locator('[data-testid="stPlotlyChart"]').first.wait_for(timeout=60000)
        page.screenshot(path=str(output / 'dashboard.png'), full_page=True)
        page.get_by_role('tab', name='Trends & patterns').click()
        page.get_by_text('Daily activity and seven-day average', exact=True).wait_for()
        page.screenshot(path=str(output / 'patterns.png'), full_page=True)
        page.get_by_role('tab', name='Source & data').click()
        page.get_by_text('A traceable public dataset', exact=True).wait_for()
        page.get_by_role('button', name='Download summary CSV').wait_for()
        page.get_by_role('tab', name='Overview', exact=True).click()
        # Native Streamlit combobox: choose Sabah and confirm a real filtered report.
        page.get_by_role('combobox', name='Collection state').click()
        page.get_by_role('option', name='Sabah', exact=True).click()
        page.get_by_text('9,896,044', exact=True).wait_for(state='hidden', timeout=120000)
        page.get_by_role('heading', name='Sabah', exact=True).wait_for(timeout=120000)
        page.get_by_role('heading', name='Monthly donation trend', exact=True).wait_for()
        assert not page.get_by_text('9,896,044', exact=True).count(), 'State filter did not change the total'
        # Check that essential content remains accessible on a narrow screen.
        mobile = browser.new_page(viewport={'width': 390, 'height': 844}, device_scale_factor=1)
        mobile.on('pageerror', lambda error: errors.append(str(error)))
        mobile.goto('http://127.0.0.1:8501', wait_until='domcontentloaded')
        mobile.get_by_text('9,896,044', exact=True).wait_for(timeout=120000)
        heading = mobile.get_by_role('heading', name='Blood donation insights', exact=True)
        heading.wait_for()
        assert heading.bounding_box()['x'] >= 0, 'Mobile heading is clipped'
        mobile.screenshot(path=str(output / 'mobile.png'), full_page=True)
        assert not errors, errors
        result = {'ok': True, 'browser_errors': errors,
                  'checks': ['real total', 'overview charts', 'pattern tab', 'source tab',
                             'CSV download control', 'state filter', 'narrow viewport'],
                  'seconds': round(perf_counter() - started, 3)}
        (output / 'visual_review.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, indent=2))
        browser.close()


if __name__ == '__main__':
    main()
```

## test_project.py

```python
"""Storage/business-rule regressions; set RUN_SPARK_TESTS=1 for Spark/UI tests."""
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import analytics
import operations as ops
from config import BLOOD_GROUPS, LOW_STOCK_THRESHOLD, SHELF_LIFE_DAYS, utcnow
from database import Store, StorageError
from seed_data import generate_dataset, insert_dataset

class StorageBase(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory(prefix='blood_bank_spark_test_')
        self.addCleanup(self.temp.cleanup)
        self.db = Store(self.temp.name)
        self.now = utcnow()
    def donor(self, group='O+', donor_id='DNR-TEST', city='Pune'):
        return ops.add_donor(self.db, 'Synthetic Test Donor', 25, 'Other', group, city,
                             '0000000001', self.now - timedelta(days=400), donor_id)
    def unit(self, unit_id='UNT-TEST', days=1, donor_id='DNR-TEST'):
        return ops.add_donation(self.db, donor_id, self.now - timedelta(days=days), unit_id)
    def request(self, quantity=1, group='O+', request_id='REQ-TEST'):
        return ops.add_request(self.db, 'Synthetic Test Hospital', group, quantity, self.now, request_id)

class RulesTests(StorageBase):
    def test_empty_storage(self):
        self.assertEqual(self.db.rows('donors'), [])
        self.assertEqual(self.db.count('donations'), 0)
    def test_donor_roundtrip(self):
        row = self.donor()
        self.assertEqual(self.db.rows('donors'), [row])
        self.assertEqual(Store(self.temp.name).rows('donors'), [row])
    def test_duplicate_donor(self):
        self.donor()
        with self.assertRaises(ValueError):
            self.donor()
        self.assertEqual(self.db.count('donors'), 1)
    def test_missing_donor(self):
        with self.assertRaises(ValueError):
            self.unit()
    def test_duplicate_unit_and_donation(self):
        self.donor()
        unit = self.unit()
        with self.assertRaises(ValueError):
            self.unit()
        with self.assertRaises(ValueError):
            ops.add_donation(self.db, 'DNR-TEST', self.now, 'UNT-OTHER', unit['donation_id'])
        self.assertEqual(self.db.count('donations'), 1)
    def test_duplicate_request(self):
        self.request()
        with self.assertRaises(ValueError):
            self.request()
    def test_date_before_registration(self):
        self.donor()
        with self.assertRaises(ValueError):
            self.unit(days=401)
    def test_expired_on_registration(self):
        self.donor()
        row = self.unit(days=SHELF_LIFE_DAYS + 1)
        self.assertEqual(row['unit_status'], 'Expired')
    def test_donation_group_copied(self):
        self.donor('AB-')
        self.assertEqual(self.unit()['blood_group'], 'AB-')
    def test_fefo_partial_and_insufficient(self):
        self.donor()
        self.unit('UNT-LATE', days=1)
        self.unit('UNT-EARLY', days=2)
        self.unit('UNT-EXPIRED', days=SHELF_LIFE_DAYS + 1)
        self.request(3)
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST', 1), ['UNT-EARLY'])
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), ['UNT-LATE'])
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), [])
        request = self.db.rows('blood_requests')[0]
        self.assertEqual((request['units_fulfilled'], request['request_status']), (2, 'Partially Fulfilled'))
    def test_exact_group_only(self):
        self.donor()
        self.unit()
        self.request(group='A+')
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), [])
    def test_complete_request(self):
        self.donor()
        self.unit()
        self.request()
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), ['UNT-TEST'])
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), [])
        self.assertEqual(self.db.rows('blood_requests')[0]['request_status'], 'Fulfilled')
    def test_tie_breaking_by_unit_id(self):
        self.donor()
        self.unit('UNT-ZZZ')
        self.unit('UNT-AAA')
        self.request()
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), ['UNT-AAA'])
    def test_expiry_exact_boundary(self):
        self.donor()
        row = self.unit()
        self.assertFalse(ops.is_available(row, row['expiration_date']))
        self.assertTrue(ops.is_available(row, row['expiration_date'] - timedelta(microseconds=1)))
    def test_issued_remains_issued(self):
        self.donor()
        self.unit()
        self.request()
        ops.fulfill_request(self.db, 'REQ-TEST')
        ops.mark_expired(self.db, self.now + timedelta(days=100))
        self.assertEqual(self.db.rows('donations')[0]['unit_status'], 'Issued')
    def test_issue_limit_validation(self):
        self.donor()
        self.unit()
        self.request()
        for value in (True, -1, 0, 1.5, '1'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ops.fulfill_request(self.db, 'REQ-TEST', value)
        self.assertEqual(self.db.rows('donations')[0]['unit_status'], 'Available')
    def test_concurrent_same_request(self):
        self.donor()
        for n in range(5):
            self.unit(f'UNT-{n:03}')
        self.request(2)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: ops.fulfill_request(Store(self.temp.name), 'REQ-TEST'), range(4)))
        self.assertEqual(sum(len(v) for v in results), 2)
        ops.validate_state(ops.state(self.db))
    def test_competing_requests(self):
        self.donor()
        for n in range(4):
            self.unit(f'UNT-{n:03}')
            self.request(2, request_id=f'REQ-{n:03}')
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda n: ops.fulfill_request(Store(self.temp.name), f'REQ-{n:03}'), range(4)))
        units = [unit for batch in results for unit in batch]
        self.assertEqual(len(units), 4)
        self.assertEqual(len(set(units)), 4)
        ops.validate_state(ops.state(self.db))
    def test_failed_publish_preserves_both_tables(self):
        self.donor()
        self.unit()
        self.request()
        before = self.db.manifest()
        with patch('database.os.replace', side_effect=OSError('injected')):
            with self.assertRaises(StorageError):
                ops.fulfill_request(self.db, 'REQ-TEST')
        self.assertEqual(self.db.manifest(), before)
        self.assertEqual(self.db.rows('donations')[0]['unit_status'], 'Available')
        self.assertEqual(self.db.rows('blood_requests')[0]['units_fulfilled'], 0)
    def test_snapshot_reader_stays_on_old_version(self):
        self.donor()
        before = self.db.manifest()
        self.unit()
        self.assertEqual(self.db.rows('donations', before), [])
        self.assertEqual(self.db.count('donations'), 1)
    def test_seed_reproducible(self):
        self.assertEqual(generate_dataset(1000, now=self.now), generate_dataset(1000, now=self.now))
    def test_seed_integrity_and_existing_refusal(self):
        data = generate_dataset(1000)
        ops.validate_state(data)
        insert_dataset(self.db, data)
        with self.assertRaises(ValueError):
            insert_dataset(self.db, data)
        self.assertEqual(self.db.count('donations'), 1000)
    def test_state_rejects_orphan(self):
        data = generate_dataset(20)
        data['donations'][0]['donor_id'] = 'DNR-MISSING'
        with self.assertRaises(ValueError):
            ops.validate_state(data)
    def test_state_rejects_overfulfillment(self):
        data = generate_dataset(20)
        data['blood_requests'][0]['units_fulfilled'] = 999
        with self.assertRaises(ValueError):
            ops.validate_state(data)
    def test_numeric_and_text_validation(self):
        for value in (None, 1, [], {}, 'x\nz'):
            with self.assertRaises(ValueError):
                ops.text_field(value, 'Name')
        for value in (True, -1, 0, 2.5, '2'):
            with self.assertRaises(ValueError):
                ops.integer(value, 'Count', 1, 10)
    def test_invalid_dates_and_groups(self):
        with self.assertRaises(ValueError):
            ops.historical_date(self.now + timedelta(days=1))
        for group in ('C+', 'o+', ''):
            with self.assertRaises(ValueError):
                ops.blood_group(group)
    def test_config_invalid(self):
        from config import positive_setting
        for value in ('bad', '0', '-1', '99999999999'):
            with patch.dict(os.environ, {'TEST_SETTING': value}), self.assertRaises(ValueError):
                positive_setting('TEST_SETTING', 42)
    def test_zero_stock_groups(self):
        self.assertEqual(len(analytics.all_group_rows([])), 8)
        self.assertEqual(analytics.all_group_rows([], 'A+'), [{'blood_group': 'A+', 'count': 0}])
    def test_date_filter_validation(self):
        for kwargs in ({'group': 'C+'}, {'start': self.now}, {'end': self.now},
                       {'start': self.now, 'end': self.now - timedelta(days=1)}):
            with self.assertRaises(ValueError):
                analytics.history_filter(**kwargs)

@unittest.skipUnless(os.getenv('RUN_SPARK_TESTS') == '1', 'Set RUN_SPARK_TESTS=1 for actual Spark.')
class SparkTests(StorageBase):
    def test_empty_dashboard(self):
        report = analytics.dashboard(self.db)
        self.assertTrue(all(v == 0 for v in report['kpis'].values()))
        self.assertEqual(len(report['low_stock']), 8)
    def test_seed_aggregates_audit_and_plan(self):
        from verify_project import audit_database
        insert_dataset(self.db, generate_dataset(1000))
        report = analytics.dashboard(self.db)
        self.assertEqual(report['kpis']['Total Donations'], 1000)
        self.assertEqual(sum(r['count'] for r in report['monthly_donations']), 1000)
        self.assertEqual(sum(r['count'] for r in report['inventory_status']), 1000)
        self.assertTrue(audit_database(self.db)['ok'])
        plan = analytics.explain_inventory(self.db)['physical_plan']
        self.assertIn('parquet', plan.lower())
        self.assertIn('Filter', plan)
    def test_filters_and_inclusive_end(self):
        self.donor()
        self.unit('UNT-OLD')
        ops.add_donation(self.db, 'DNR-TEST', self.now.date(), 'UNT-TODAY')
        result = analytics.dashboard(self.db, 'O+', self.now.date(), self.now.date())
        self.assertEqual(sum(r['count'] for r in result['monthly_donations']), 1)
        self.assertEqual(result['kpis']['Total Donations'], 2)
        self.assertEqual(result['available_by_group'], [{'blood_group': 'O+', 'count': 2}])
    def test_literal_search_not_sql_injection(self):
        self.donor(city="O'Connor")
        self.assertEqual(ops.list_donors(self.db, city="O'Connor")[0], 1)
        self.assertEqual(ops.list_donors(self.db, city="' OR true --")[0], 0)
        self.assertEqual(ops.list_donors(self.db, city='.*')[0], 0)
    def test_pagination(self):
        insert_dataset(self.db, generate_dataset(220))
        total, first = ops.list_donors(self.db)
        _, second = ops.list_donors(self.db, skip=100)
        self.assertEqual(total, 110)
        self.assertEqual(len(second), 10)
        self.assertFalse({r['donor_id'] for r in first} & {r['donor_id'] for r in second})
    def test_low_stock_and_expiry(self):
        self.donor()
        for i in range(LOW_STOCK_THRESHOLD):
            self.unit(f'UNT-{i:03}')
        self.unit('UNT-EXPIRED', SHELF_LIFE_DAYS + 1)
        self.unit('UNT-NEAR', SHELF_LIFE_DAYS - 2)
        result = analytics.dashboard(self.db)
        self.assertEqual(result['kpis']['Expired Blood Units'], 1)
        self.assertEqual(result['kpis']['Blood Units Near Expiration'], 1)
        self.assertNotIn('O+', [r['blood_group'] for r in result['low_stock']])
    def test_ui_forms_and_navigation(self):
        import streamlit as st
        from streamlit.testing.v1 import AppTest
        st.cache_resource.clear()
        try:
            with patch('database.connect', return_value=self.db):
                app = AppTest.from_file('app.py', default_timeout=120).run()
                self.assertFalse(app.exception)
                app.sidebar.selectbox[0].set_value('Inventory demo (simulated)').run()
                def field(items, label):
                    return next(x for x in items if x.label == label)
                app.sidebar.radio[0].set_value('Donor Management').run()
                field(app.text_input, 'Donor ID (blank = automatic)').set_value('DNR-UI')
                field(app.text_input, 'Name').set_value('Synthetic UI Donor')
                field(app.text_input, 'City').set_value('Pune')
                field(app.button, 'Register donor').click().run()
                self.assertEqual(self.db.count('donors'), 1)
                app.sidebar.radio[0].set_value('Blood Inventory').run()
                field(app.text_input, 'Existing donor ID').set_value('DNR-UI')
                field(app.text_input, 'Unit ID (blank = automatic)').set_value('UNT-UI')
                field(app.button, 'Record donation').click().run()
                self.assertEqual(self.db.count('donations'), 1)
                app.sidebar.radio[0].set_value('Blood Requests').run()
                field(app.text_input, 'Request ID (blank = automatic)').set_value('REQ-UI')
                field(app.text_input, 'Synthetic hospital name').set_value('Synthetic UI Hospital')
                field(app.button, 'Add request').click().run()
                field(app.text_input, 'Request ID to fulfill').set_value('REQ-UI')
                field(app.button, 'Issue available units').click().run()
                self.assertEqual(self.db.rows('blood_requests')[0]['request_status'], 'Fulfilled')
                app.sidebar.radio[0].set_value('Analytics').run()
                field(app.button, 'Run explain comparison').click().run()
                self.assertFalse(app.exception)
                self.assertFalse(app.error)
        finally:
            st.cache_resource.clear()

if __name__ == '__main__':
    unittest.main(verbosity=2)
```

## test_public_data.py

```python
"""Public-source parsing, import safety, aggregation and Streamlit regressions.

Tiny fixtures are deliberately synthetic test cases, not the included real CSV.
MongoDB tests remove only their own random blood_bank_test_<uuid> database.
"""
import csv
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pandas as pd
from database import Store, StorageError

import public_data as source
from database import connect


def fixture_rows():
    return [[day, state, group, value]
            for day in ('2025-01-01', '2025-02-01')
            for state in ('Johor', 'Sabah')
            for group, value in [('a', '2'), ('b', '3'), ('ab', '1'), ('o', '3'), ('all', '10')]]


class PublicPureTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / 'fixture.csv'

    def write_fixture(self, rows=None, columns=None):
        with self.path.open('w', newline='', encoding='utf-8') as output:
            writer = csv.writer(output)
            writer.writerow(columns or ['date', 'state', 'blood_type', 'donations'])
            writer.writerows(fixture_rows() if rows is None else rows)
        return self.path

    def test_metadata_preserves_published_discrepancy(self):
        self.write_fixture()
        manifest = source.describe_csv(source.load_csv(self.path), self.path)
        self.assertEqual(manifest['rows'], 20)
        self.assertEqual(manifest['published_total_donations'], 40)
        self.assertEqual(manifest['abo_breakdown_total'], 36)
        self.assertEqual(manifest['date_state_rows_with_difference'], 4)

    def test_invalid_numeric_counts_rejected(self):
        for value in ('-1', '1.5', '', 'NaN', 'true', '1000000000'):
            rows = fixture_rows()
            rows[0][3] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                source.load_csv(self.write_fixture(rows))

    def test_invalid_dates_states_groups_rejected(self):
        for position, value in ((0, '2025-02-30'), (0, '2999-01-01'), (0, '01/01/2025'),
                                (1, 'Pune'), (2, 'O+')):
            rows = fixture_rows()
            rows[0][position] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                source.load_csv(self.write_fixture(rows))

    def test_duplicates_and_missing_categories_rejected(self):
        for rows in (fixture_rows() + [fixture_rows()[0]], fixture_rows()[1:], []):
            with self.assertRaises(ValueError):
                source.load_csv(self.write_fixture(rows))

    def test_changed_schema_rejected(self):
        with self.assertRaises(ValueError):
            source.load_csv(self.write_fixture(columns=['date', 'state', 'blood_type', 'units']))

    def test_filter_validation_and_inclusive_day(self):
        day = date(2025, 1, 1)
        for kwargs in ({'group': 'O+'}, {'state': 'Delhi'}, {'start': day},
                       {'start': date(2025, 2, 1), 'end': day}):
            with self.assertRaises(ValueError):
                source.public_filter(**kwargs)
        query = source.public_filter(day, day)
        self.assertIn("blood_type = 'all'", query)
        self.assertIn('2025-01-02 00:00:00', query)

    def test_existing_download_never_overwritten(self):
        self.write_fixture()
        original = self.path.read_bytes()
        with patch('public_data.urlopen', side_effect=AssertionError('Should not download')):
            source.download_dataset(self.path)
        self.assertEqual(self.path.read_bytes(), original)

    def test_invalid_download_not_accepted(self):
        from io import BytesIO
        with patch('public_data.urlopen', return_value=BytesIO(b'<html>Error</html>')):
            with self.assertRaises(ValueError):
                source.download_dataset(self.path)
        self.assertFalse(self.path.exists())
        self.assertEqual(list(self.path.parent.iterdir()), [])



class PublicStorageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='blood_bank_spark_test_')
        self.addCleanup(self.temporary.cleanup)
        self.db = Store(self.temporary.name)
        self.frame = pd.DataFrame(fixture_rows(), columns=['date', 'state', 'blood_type', 'donations'])
        self.frame['date'] = pd.to_datetime(self.frame['date'], utc=True)
        self.frame['donations'] = self.frame['donations'].astype('int64')
        self.manifest = {'sha256': 'test-fixture-only'}
        source.import_snapshot(self.db, self.frame, self.manifest)

    def test_idempotency_full_verification(self):
        self.assertTrue(source.import_snapshot(self.db, self.frame, self.manifest)['already_present'])
        self.assertEqual(source.verify_snapshot(self.db, self.frame, self.manifest)['verified_rows'], 20)

    def test_existing_snapshot_requires_confirmation(self):
        with self.assertRaises(ValueError):
            source.import_snapshot(self.db, self.frame, {'sha256': 'different'})
        self.assertTrue(source.verify_snapshot(self.db, self.frame, self.manifest)['ok'])

    def test_replacement_and_old_snapshot_preserved(self):
        before = self.db.manifest()
        source.import_snapshot(self.db, self.frame, {'sha256': 'different'}, replace=True)
        self.assertTrue(source.verify_snapshot(self.db, self.frame, self.manifest, before)['ok'])
        self.assertTrue(source.verify_snapshot(self.db, self.frame, {'sha256': 'different'})['ok'])

    def test_failed_write_preserves_live_data(self):
        before = self.db.manifest()
        with patch('database.pq.write_table', side_effect=OSError('injected')):
            with self.assertRaises(StorageError):
                source.import_snapshot(self.db, self.frame, {'sha256': 'new'}, replace=True)
        self.assertEqual(self.db.manifest(), before)
        self.assertTrue(source.verify_snapshot(self.db, self.frame, self.manifest)['ok'])

    def test_verifier_and_repeat_import_detect_corruption(self):
        rows = self.db.rows(source.COLLECTION)
        rows[0]['donations'] = 999
        with self.db.locked():
            self.db.publish({source.COLLECTION: rows})
        with self.assertRaisesRegex(ValueError, 'Source mismatch'):
            source.verify_snapshot(self.db, self.frame, self.manifest)
        with self.assertRaisesRegex(ValueError, 'Source mismatch'):
            source.import_snapshot(self.db, self.frame, self.manifest)

    def test_verifier_detects_missing_row(self):
        with self.db.locked():
            self.db.publish({source.COLLECTION: self.db.rows(source.COLLECTION)[1:]})
        with self.assertRaises(ValueError):
            source.verify_snapshot(self.db, self.frame, self.manifest)

    @unittest.skipUnless(os.getenv('RUN_SPARK_TESTS') == '1', 'Set RUN_SPARK_TESTS=1.')
    def test_six_sql_queries_no_double_counting(self):
        result = source.public_report(self.db)
        self.assertEqual((result['total'], result['abo_total'], result['total_minus_abo']), (40, 36, 4))
        self.assertEqual((result['rows'], result['days']), (4, 2))
        for name in ('monthly', 'yearly', 'daily', 'states', 'weekdays'):
            self.assertEqual(sum(row['count'] for row in result[name]), 40)
        self.assertIn('parquet', source.explain_public(self.db)['physical_plan'].lower())

    @unittest.skipUnless(os.getenv('RUN_SPARK_TESTS') == '1', 'Set RUN_SPARK_TESTS=1.')
    def test_sql_filters_and_empty(self):
        result = source.public_report(self.db, date(2025, 1, 1), date(2025, 1, 1), 'Sabah', 'a')
        self.assertEqual(result['total'], 2)
        self.assertEqual(result['groups'], [{'blood_group': 'A', 'count': 2}])
        self.assertEqual(source.public_report(self.db, state='Sarawak')['total'], 0)

    @unittest.skipUnless(os.getenv('RUN_SPARK_TESTS') == '1', 'Set RUN_SPARK_TESTS=1.')
    def test_public_ui(self):
        import streamlit as st
        from streamlit.testing.v1 import AppTest
        st.cache_resource.clear()
        try:
            with patch('database.connect', return_value=self.db):
                app = AppTest.from_file('app.py', default_timeout=120).run()
                self.assertEqual(app.metric[0].value, '40')
                next(x for x in app.selectbox if x.label == 'Collection state').set_value('Sabah').run()
                self.assertEqual(app.metric[0].value, '20')
                app.sidebar.radio[0].set_value('Analytics').run()
                next(x for x in app.button if x.label == 'Explain public dataset query').click().run()
                self.assertFalse(app.exception)
                self.assertFalse(app.error)
        finally:
            st.cache_resource.clear()

if __name__ == '__main__':
    unittest.main(verbosity=2)
```

## test_robustness.py

```python
"""Failure injection, process locks and platform regressions for Spark migration."""
import json
import os
import subprocess
import sys
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
import operations as ops
from database import Store, StorageError
from test_project import StorageBase

class RobustTests(StorageBase):
    def test_query_cache_invalidates_on_new_snapshot(self):
        with patch.object(Store, '_query', return_value=[{'count': 1}]) as query:
            self.assertEqual(self.db.query('SELECT count(*) AS count FROM donors')[0]['count'], 1)
            self.db.query('SELECT count(*) AS count FROM donors')
            self.assertEqual(query.call_count, 1)
            self.donor()
            self.db.query('SELECT count(*) AS count FROM donors')
            self.assertEqual(query.call_count, 2)

    def test_corrupt_parquet_is_friendly(self):
        self.donor()
        path = self.db.root / self.db.manifest()['tables']['donors']
        path.write_bytes(b'corrupt test fixture')
        with self.assertRaises(StorageError):
            self.db.count('donors')
        with self.assertRaises(StorageError):
            self.db.read_table('donors')

    def test_scan_setup_failure_is_sanitized(self):
        with patch('database.spark_session', side_effect=RuntimeError('internal failure sentinel')):
            with self.assertLogs(level='ERROR'), self.assertRaises(StorageError) as caught:
                self.db.query('SELECT COUNT(*) FROM donors')
        self.assertNotIn('internal failure sentinel', str(caught.exception))

    def test_corrupt_manifest_fails_closed(self):
        self.donor()
        (self.db.root / 'current.json').write_text('{broken', encoding='utf-8')
        with self.assertRaises(StorageError):
            self.db.rows('donors')
    def test_manifest_path_escape_rejected(self):
        self.donor()
        value = self.db.manifest()
        value['tables']['donors'] = '../../outside.parquet'
        (self.db.root / 'current.json').write_text(json.dumps(value), encoding='utf-8')
        with self.assertRaises(StorageError):
            self.db.manifest()
    def test_missing_parquet_fails_closed(self):
        self.donor()
        value = self.db.manifest()
        value['tables']['donors'] = 'versions/missing/donors.parquet'
        (self.db.root / 'current.json').write_text(json.dumps(value), encoding='utf-8')
        with self.assertRaises(StorageError):
            self.db.manifest()
    def test_invalid_ids_and_contacts(self):
        for value in (None, [], 1, 'bad id'):
            with self.assertRaises(ValueError):
                ops.readable_id(value, 'DNR')
        for value in (123, None, '123', '123456789a'):
            with self.assertRaises(ValueError):
                ops.add_donor(self.db, 'Synthetic Donor', 25, 'Other', 'O+', 'Pune', value, self.now)
    def test_whitespace_id_auto_generated(self):
        self.assertTrue(ops.readable_id('  ', 'DNR').startswith('DNR-'))
    def test_invalid_seed_sizes(self):
        from seed_data import generate_dataset
        for value in (True, 19, 2.5, None, '20'):
            with self.assertRaises(ValueError):
                generate_dataset(value)
    def test_failed_ack_retry_does_not_overissue(self):
        self.donor()
        self.unit('UNT-FIRST')
        self.unit('UNT-SECOND')
        self.request()
        original = os.replace
        def acknowledged_lost(*args, **kwargs):
            original(*args, **kwargs)
            raise OSError('simulated acknowledgement lost after publish')
        with patch('database.os.replace', side_effect=acknowledged_lost):
            with self.assertRaises(StorageError):
                ops.fulfill_request(self.db, 'REQ-TEST')
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), [])
        ops.validate_state(ops.state(self.db))
        self.assertEqual(sum(r['unit_status'] == 'Issued' for r in self.db.rows('donations')), 1)
    def test_independent_process_writers_cannot_overissue(self):
        self.donor()
        for n in range(4):
            self.unit(f'UNT-{n:03}')
        self.request(2)
        code = "import sys; from database import Store; from operations import fulfill_request; print(len(fulfill_request(Store(sys.argv[1]), 'REQ-TEST')))"
        processes = [subprocess.Popen([sys.executable, '-c', code, self.temp.name],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(3)]
        try:
            outcomes = [p.communicate(timeout=45) for p in processes]
            self.assertTrue(all(p.returncode == 0 for p in processes), outcomes)
            self.assertEqual(sum(int(out.strip()) for out, _ in outcomes), 2)
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait()
        ops.validate_state(ops.state(self.db))
    def test_reset_cancel_preserves_data(self):
        self.donor()
        env = {**os.environ, 'SPARK_DATA_DIR': str(Path(self.temp.name).parent)}
        # Use its own root namespace, never the live project namespace.
        from tempfile import TemporaryDirectory
        with TemporaryDirectory(prefix='blood_bank_cli_test_') as folder:
            db = Store(Path(folder) / 'management')
            ops.add_donor(db, 'Synthetic CLI Donor', 25, 'Other', 'O+', 'Pune', '0000000001', self.now)
            result = subprocess.run([sys.executable, 'seed_data.py', '--records', '20', '--reset'],
                                    input='cancel\n', text=True, capture_output=True,
                                    env={**os.environ, 'SPARK_DATA_DIR': folder}, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(db.count('donors'), 1)
    def test_bad_configuration_ui_friendly(self):
        code = ('from streamlit.testing.v1 import AppTest; a=AppTest.from_file("app.py",default_timeout=30).run(); '
                'assert not a.exception; assert len(a.error)==1; assert "Invalid configuration" in a.error[0].value')
        result = subprocess.run([sys.executable, '-c', code], text=True, capture_output=True,
                                env={**os.environ, 'SHELF_LIFE_DAYS': 'bad'}, timeout=45)
        self.assertEqual(result.returncode, 0, result.stderr)
    def test_expiration_rechecked_during_issue(self):
        self.donor()
        self.unit()
        self.request()
        expires = self.db.rows('donations')[0]['expiration_date']
        with patch('operations.utcnow', return_value=expires):
            self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), [])
    def test_storage_error_ui_friendly(self):
        import streamlit as st
        from streamlit.testing.v1 import AppTest
        st.cache_resource.clear()
        try:
            with patch('database.connect', side_effect=StorageError('injected')):
                app = AppTest.from_file('app.py', default_timeout=30).run()
                self.assertFalse(app.exception)
                self.assertEqual(len(app.error), 1)
        finally:
            st.cache_resource.clear()

if __name__ == '__main__':
    unittest.main(verbosity=2)
```

## test_presentation.py

```python
"""Guard analytical interpretation in the simplified real-data dashboard."""
import unittest
import math
from datetime import date
import presentation as view


class PresentationTests(unittest.TestCase):
    def test_partial_month_gets_an_explicit_marker(self):
        rows = [{'month': '2025-01', 'count': 30}, {'month': '2025-02', 'count': 10}]
        figure = view.monthly_figure(rows, date(2025, 1, 1), date(2025, 2, 10))
        self.assertEqual(list(figure.data[1].x), ['2025-02'])
        self.assertEqual(figure.data[1].name, 'Partial calendar month')

    def test_partial_year_gets_a_different_color(self):
        figure = view.yearly_figure([{'year': '2024', 'count': 100}, {'year': '2025', 'count': 10}],
                                   date(2024, 1, 1), date(2025, 2, 10))
        self.assertNotEqual(figure.data[0].marker.color[0], figure.data[0].marker.color[1])

    def test_findings_use_selected_denominators(self):
        report = {'total': 100, 'abo_total': 80,
                  'states': [{'state': 'Johor', 'count': 60}],
                  'groups': [{'blood_group': 'A', 'count': 40}, {'blood_group': 'O', 'count': 40}],
                  'daily': [{'day': '2025-01-01', 'count': 100}]}
        items = view.findings(report)
        self.assertIn('60.0%', items[0][2])
        self.assertIn('50.0%', items[1][2])  # ABO denominator, not published-total denominator

    def test_no_findings_for_empty_selection(self):
        self.assertEqual(view.findings({'total': 0, 'abo_total': 0, 'states': [], 'groups': [], 'daily': []}), [])

    def test_partial_months_not_used_for_comparison(self):
        rows = [{'month': '2024-01', 'count': 100}, {'month': '2025-01', 'count': 200}]
        self.assertIsNone(view.complete_month_change(rows, date(2024, 1, 1), date(2025, 1, 10)))
        self.assertEqual(view.complete_month_change(rows, date(2024, 1, 1), date(2025, 1, 31)), ('2025-01', 1.0))

    def test_zero_baseline_has_no_percentage(self):
        rows = [{'month': '2024-01', 'count': 0}, {'month': '2025-01', 'count': 10}]
        self.assertIsNone(view.complete_month_change(rows, date(2024, 1, 1), date(2025, 1, 31)))

    def test_weekday_average_normalizes_observed_dates(self):
        figure = view.weekday_figure([{'weekday': 1, 'count': 30}],
            [{'day': '2025-01-06', 'count': 10}, {'day': '2025-01-13', 'count': 20}])
        self.assertEqual(figure.data[0].y[0], 15)
        self.assertTrue(math.isnan(figure.data[0].y[1]))

    def test_daily_average_does_not_bridge_missing_dates(self):
        rows = [{'day': f'2025-01-{day:02}', 'count': 10} for day in (1, 2, 3, 5, 6, 7, 8)]
        figure = view.daily_figure(rows)
        self.assertTrue(all(value != value for value in figure.data[1].y))

    def test_daily_average_after_seven_complete_days(self):
        rows = [{'day': f'2025-01-{day:02}', 'count': day} for day in range(1, 8)]
        self.assertEqual(view.daily_figure(rows).data[1].y[-1], 4)


if __name__ == '__main__':
    unittest.main(verbosity=2)
```

## requirements.txt

```text
pyspark==4.0.1
pyarrow>=23,<24
streamlit>=1.52,<2
pandas>=2.2,<3
plotly>=5.24,<7
Faker>=33,<41
python-dotenv>=1.0,<2
```

## .env.example

```text
# Apache Spark + local Parquet. MongoDB is not required to run the application.
SPARK_MASTER=local[2]
# Optional absolute paths (defaults are inside the project):
# SPARK_DATA_DIR=C:/Users/avira/Desktop/bda/data/spark
# SPARK_JAVA_HOME=C:/Program Files/Eclipse Adoptium/jdk-21
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
.runtime/
data/spark/
artifacts/
spark-warehouse/
derby.log
metastore_db/
backups/
qa_results/debug.png
```

## .streamlit/config.toml

```toml
[theme]
base = "light"
primaryColor = "#B42335"
backgroundColor = "#F5F7FB"
secondaryBackgroundColor = "#FFFFFF"
textColor = "#253247"
font = "sans serif"

[browser]
gatherUsageStats = false
```

## .gitattributes

```text
* text=auto
*.py text eol=lf
*.md text eol=lf
*.txt text eol=lf
*.toml text eol=lf
*.json text eol=lf
data/raw/*.csv -text
*.png binary
```
