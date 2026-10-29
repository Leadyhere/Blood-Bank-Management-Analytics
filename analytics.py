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
