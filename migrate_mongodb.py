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
