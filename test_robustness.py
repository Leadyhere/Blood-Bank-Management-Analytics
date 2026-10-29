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
