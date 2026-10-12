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
