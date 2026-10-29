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
