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
