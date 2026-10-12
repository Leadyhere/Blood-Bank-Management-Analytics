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
