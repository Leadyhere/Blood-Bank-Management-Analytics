"""Regression, outage, concurrency and seed-safety tests for the audit."""
import os
import subprocess
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

from pymongo.errors import AutoReconnect, ConfigurationError

import analytics
import database
import operations as ops
import seed_data
from config import positive_setting, utcnow
from verify_project import audit_database


class RobustPureTests(unittest.TestCase):
    def test_required_text_rejects_non_strings_and_controls(self):
        for value in (None, 123, [], {}, 'A\x00B', 'A\nB'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ops.text_field(value, 'Name')

    def test_ids_reject_non_strings_and_normalize_whitespace(self):
        for value in (None, 123, [], {}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ops.readable_id(value, 'DNR')
        self.assertTrue(ops.readable_id('   ', 'DNR').startswith('DNR-'))

    def test_history_filters_validate_group_and_both_dates(self):
        for kwargs in ({'group': 'INVALID'}, {'start': utcnow().date()}, {'end': utcnow().date()}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                analytics.history_filter(**kwargs)

    def test_filtered_groups_do_not_appear_as_false_zeroes(self):
        self.assertEqual(analytics.all_group_rows([{'blood_group': 'O+', 'count': 3}], 'O+'),
                         [{'blood_group': 'O+', 'count': 3}])

    def test_constructor_failure_is_sanitized(self):
        with patch('database.MongoClient', side_effect=ConfigurationError('private-uri-sentinel')):
            with self.assertRaises(ConnectionError) as caught:
                database.connect()
        self.assertNotIn('private-uri-sentinel', str(caught.exception))

    def test_failed_ping_closes_client(self):
        client = MagicMock()
        client.admin.command.side_effect = AutoReconnect('private-uri-sentinel')
        with patch('database.MongoClient', return_value=client):
            with self.assertRaises(ConnectionError) as caught:
                database.connect()
        client.close.assert_called_once()
        self.assertNotIn('private-uri-sentinel', str(caught.exception))

    def test_large_shelf_life_preserves_registration_order(self):
        with patch('seed_data.SHELF_LIFE_DAYS', 1000):
            data = seed_data.generate_dataset(1000)
        registered = {row['donor_id']: row['registration_date'] for row in data['donors']}
        self.assertTrue(all(row['donation_date'] >= registered[row['donor_id']]
                            for row in data['donations']))

    def test_invalid_seed_sizes_are_rejected(self):
        for size in (True, 19, 20.5, '100', None):
            with self.subTest(size=size), self.assertRaises(ValueError):
                seed_data.generate_dataset(size)

    def test_default_seed_all_states_at_25000(self):
        data = seed_data.generate_dataset(25000)
        self.assertEqual({row['unit_status'] for row in data['donations']}, {'Available', 'Issued', 'Expired'})

    def test_settings_reject_bad_or_extreme_values(self):
        for value in ('abc', '0', '-1', '99999999999999999'):
            with self.subTest(value=value), patch.dict(os.environ, {'BBA_TEST_SETTING': value}):
                with self.assertRaisesRegex(ValueError, 'BBA_TEST_SETTING'):
                    positive_setting('BBA_TEST_SETTING', 42)

    def test_client_uses_bounded_operation_timeout(self):
        client = MagicMock()
        with patch('database.MongoClient', return_value=client) as constructor:
            database.connect()
        self.assertEqual(constructor.call_args.kwargs['socketTimeoutMS'], 10000)
        self.assertEqual(constructor.call_args.kwargs['timeoutMS'], 10000)

    def test_invalid_database_name_closes_client(self):
        client = MagicMock()
        client.__getitem__.side_effect = ValueError('private-db-sentinel')
        with patch('database.MongoClient', return_value=client):
            with self.assertRaises(ConnectionError) as caught:
                database.connect()
        client.close.assert_called_once()
        self.assertNotIn('private-db-sentinel', str(caught.exception))

    def test_unacknowledged_writes_are_rejected(self):
        client = MagicMock()
        client.__getitem__.return_value.write_concern.acknowledged = False
        with patch('database.MongoClient', return_value=client):
            with self.assertRaises(ConnectionError):
                database.connect()
        client.close.assert_called_once()

    def test_pagination_rejects_unbounded_or_negative_limits(self):
        for kwargs in ({'limit': 0}, {'limit': -1}, {'skip': -1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                ops.list_donors(MagicMock(), **kwargs)

    def test_bad_configuration_displays_error_in_streamlit(self):
        code = ('from streamlit.testing.v1 import AppTest; '
                'app=AppTest.from_file("app.py", default_timeout=30).run(); '
                'assert len(app.exception)==0; assert len(app.error)==1; '
                'assert "Invalid configuration" in app.error[0].value')
        result = subprocess.run([sys.executable, '-c', code], env={**os.environ, 'SHELF_LIFE_DAYS': 'bad'},
                                text=True, capture_output=True, timeout=45)
        self.assertEqual(result.returncode, 0, result.stderr)


@unittest.skipUnless(os.getenv('RUN_MONGO_TESTS') == '1', 'Set RUN_MONGO_TESTS=1.')
class RobustMongoTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.name = 'blood_bank_test_' + uuid4().hex
        cls.client, cls.db = database.connect(cls.name)
        database.create_indexes(cls.db)

    @classmethod
    def tearDownClass(cls):
        assert cls.db.name == cls.name and cls.name.startswith('blood_bank_test_')
        cls.client.drop_database(cls.name)
        cls.client.close()

    def setUp(self):
        for collection in ('donors', 'donations', 'blood_requests'):
            self.db[collection].delete_many({})
        self.now = utcnow()
        ops.add_donor(self.db, 'Synthetic Robustness Donor', 25, 'Other', 'O+', 'Pune',
                      '0000000001', self.now - timedelta(days=400), 'DNR-ROBUST')

    def unit(self, name='UNT-ROBUST'):
        return ops.add_donation(self.db, 'DNR-ROBUST', self.now - timedelta(days=1), name)

    def request(self, name='REQ-ROBUST', quantity=1):
        return ops.add_request(self.db, 'Synthetic Test Hospital', 'O+', quantity, self.now, name)

    def test_application_clock_lag_cannot_issue_expired_unit(self):
        unit = self.unit()
        self.request()
        self.db.donations.update_one({'_id': unit['_id']}, {'$set': {
            'expiration_date': self.now - timedelta(seconds=1), 'unit_status': 'Available'}})
        with patch('operations.utcnow', return_value=self.now - timedelta(seconds=10)):
            self.assertEqual(ops.fulfill_request(self.db, 'REQ-ROBUST'), [])

    def test_request_id_duplicates(self):
        self.request()
        with self.assertRaises(ValueError):
            self.request()
        self.assertEqual(self.db.blood_requests.count_documents({}), 1)

    def test_donor_id_invalid_type_is_friendly(self):
        with self.assertRaises(ValueError):
            ops.add_donation(self.db, None, self.now)

    def test_failed_acknowledgement_retry_does_not_overissue(self):
        self.unit('UNT-FIRST')
        self.unit('UNT-SECOND')
        self.request()
        original = self.db.donations.find_one_and_update
        def issued_but_ack_lost(*args, **kwargs):
            original(*args, **kwargs)
            raise AutoReconnect('simulated lost acknowledgement')
        with patch.object(type(self.db.donations), 'find_one_and_update', side_effect=issued_but_ack_lost):
            with self.assertRaises(AutoReconnect):
                ops.fulfill_request(self.db, 'REQ-ROBUST')
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-ROBUST'), [])
        self.assertEqual(self.db.donations.count_documents({'unit_status': 'Issued'}), 1)

    def test_competing_requests_do_not_share_units(self):
        for i in range(7):
            self.unit(f'UNT-{i:03d}')
        for i in range(10):
            self.request(f'REQ-{i:03d}', 2)
        with ThreadPoolExecutor(max_workers=10) as pool:
            issued = list(pool.map(lambda i: ops.fulfill_request(self.db, f'REQ-{i:03d}'), range(10)))
        flattened = [unit for batch in issued for unit in batch]
        self.assertEqual(len(flattened), 7)
        self.assertEqual(len(set(flattened)), 7)
        self.assertTrue(all(row['units_fulfilled'] <= 2 for row in self.db.blood_requests.find({})))

    def test_fefo_equal_expiry_uses_unit_id(self):
        self.unit('UNT-ZZZ')
        self.unit('UNT-AAA')
        self.request()
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-ROBUST'), ['UNT-AAA'])

    def test_pagination_is_disjoint(self):
        for i in range(105):
            ops.add_donor(self.db, 'Synthetic Paging Donor', 25, 'Other', 'A+', 'Pune',
                          '0000000001', self.now, f'DNR-PAGE-{i:03d}')
        total, first = ops.list_donors(self.db, limit=100)
        _, second = ops.list_donors(self.db, skip=100, limit=100)
        self.assertEqual(total, 106)
        self.assertEqual(len(first), 100)
        self.assertEqual(len(second), 6)
        self.assertFalse({r['donor_id'] for r in first} & {r['donor_id'] for r in second})

    def test_seed_refuses_existing_data_and_cancel_preserves_it(self):
        env = {**os.environ, 'MONGODB_DB': self.name}
        for args, answer in [([], None), (['--reset'], 'cancel\n')]:
            result = subprocess.run([sys.executable, 'seed_data.py', '--records', '20', *args],
                                    env=env, input=answer, text=True, capture_output=True, timeout=30)
            self.assertEqual(result.returncode, 0 if args else 1)
            self.assertEqual(self.db.donors.count_documents({}), 1)
            self.assertEqual(self.db.donations.count_documents({}), 0)

    def test_streamlit_connection_error_is_friendly(self):
        import streamlit as st
        from streamlit.testing.v1 import AppTest
        st.cache_resource.clear()
        with patch('database.connect', side_effect=ConnectionError('Cannot connect to MongoDB.')):
            app = AppTest.from_file('app.py', default_timeout=30).run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(len(app.error), 1)
        st.cache_resource.clear()

    def test_auditor_reports_manual_corruption_without_repairing(self):
        unit = self.unit()
        self.request()
        self.db.donations.update_one({'_id': unit['_id']}, {'$set': {'donor_id': 'DNR-MISSING'}})
        self.db.blood_requests.update_one({}, {'$set': {'units_fulfilled': 99}})
        result = audit_database(self.db)
        self.assertFalse(result['ok'])
        self.assertGreaterEqual(result['error_count'], 2)
        self.assertEqual(self.db.blood_requests.find_one({})['units_fulfilled'], 99)

    def test_invalid_issue_limit_changes_nothing(self):
        self.unit()
        self.request()
        for value in (0, -1, True, '2', 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ops.fulfill_request(self.db, 'REQ-ROBUST', value)
        self.assertEqual(self.db.donations.count_documents({'unit_status': 'Issued'}), 0)

    def test_auditor_tolerates_invalid_reference_type(self):
        self.request()
        self.db.blood_requests.update_one({}, {'$set': {'request_id': ['not-a-string']}})
        self.assertFalse(audit_database(self.db)['ok'])

    def test_confirmed_reset_only_replaces_named_collections(self):
        self.db.audit_sentinel.insert_one({'keep': True})
        env = {**os.environ, 'MONGODB_DB': self.name}
        result = subprocess.run([sys.executable, 'seed_data.py', '--records', '20', '--reset'],
                                env=env, input=f'RESET {self.name}\n', text=True,
                                capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.db.donations.count_documents({}), 20)
        self.assertEqual(self.db.donors.count_documents({}), 10)
        self.assertEqual(self.db.blood_requests.count_documents({}), 4)
        self.assertEqual(self.db.audit_sentinel.count_documents({}), 1)

    def test_date_and_group_filters_have_exact_scope(self):
        self.unit('UNT-EARLIER')
        ops.add_donation(self.db, 'DNR-ROBUST', self.now.date(), 'UNT-TODAY')
        ops.add_donor(self.db, 'Synthetic Other Group', 25, 'Other', 'A+', 'Delhi',
                      '0000000002', self.now - timedelta(days=400), 'DNR-OTHER')
        ops.add_donation(self.db, 'DNR-OTHER', self.now.date(), 'UNT-OTHER')
        result = analytics.dashboard(self.db, 'O+', start=self.now.date(), end=self.now.date())
        self.assertEqual(result['available_by_group'], [{'blood_group': 'O+', 'count': 2}])
        self.assertEqual(sum(row['count'] for row in result['monthly_donations']), 1)
        self.assertEqual(result['kpis']['Total Donations'], 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
