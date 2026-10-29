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
