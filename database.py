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
