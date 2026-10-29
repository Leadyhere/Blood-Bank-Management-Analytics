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
