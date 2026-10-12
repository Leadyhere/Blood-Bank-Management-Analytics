"""Standard-library tests. Set RUN_MONGO_TESTS=1 for real MongoDB/UI tests.

Integration tests create and remove only a random blood_bank_test_<uuid> DB.
They never delete the demonstration database.
"""
import os
import unittest
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4

import analytics
import operations as ops
from config import BLOOD_GROUPS, LOW_STOCK_THRESHOLD, SHELF_LIFE_DAYS, utcnow
from database import connect, create_indexes
from seed_data import generate_dataset, insert_dataset


class PureTests(unittest.TestCase):
    def test_validation(self):
        for value in ('C+', '', 'o+'):
            with self.assertRaises(ValueError):
                ops.blood_group(value)
        for value in (0, -1, 1.5, True):
            with self.assertRaises(ValueError):
                ops.integer(value, 'Quantity', 1, 100)
        with self.assertRaises(ValueError):
            ops.historical_date(utcnow() + timedelta(days=2))
        with self.assertRaises(ValueError):
            ops.readable_id('bad id', 'DNR')

    def test_zero_stock_groups(self):
        rows = analytics.all_group_rows([])
        self.assertEqual([row['blood_group'] for row in rows], list(BLOOD_GROUPS))
        self.assertEqual(sum(row['count'] for row in rows), 0)

    def test_inclusive_date_filter(self):
        today = utcnow().date()
        query = analytics.history_filter(start=today, end=today)
        self.assertEqual(query['donation_date']['$lt'] - query['donation_date']['$gte'], timedelta(days=1))

    def test_reproducible_generation(self):
        now = utcnow()
        self.assertEqual(generate_dataset(100, 42, now), generate_dataset(100, 42, now))

    def test_seed_integrity(self):
        now = utcnow()
        data = generate_dataset(1000, now=now)
        donors = {row['donor_id']: row for row in data['donors']}
        requests = {row['request_id']: row for row in data['blood_requests']}
        fulfilled = Counter()
        self.assertEqual(len(data['donations']), 1000)
        self.assertEqual(len(donors), 500)
        self.assertEqual(len(requests), 200)
        self.assertEqual(len({r['unit_id'] for r in data['donations']}), 1000)
        self.assertEqual(len({r['donation_id'] for r in data['donations']}), 1000)
        self.assertEqual(len({r['_id'] for rows in data.values() for r in rows}), 1700)
        for unit in data['donations']:
            donor = donors[unit['donor_id']]
            self.assertEqual(unit['blood_group'], donor['blood_group'])
            self.assertGreaterEqual(unit['donation_date'], donor['registration_date'])
            self.assertEqual(unit['expiration_date'] - unit['donation_date'], timedelta(days=SHELF_LIFE_DAYS))
            if unit['unit_status'] == 'Issued':
                request = requests[unit['request_id']]
                self.assertEqual(unit['blood_group'], request['blood_group'])
                self.assertLessEqual(unit['donation_date'], unit['issued_at'])
                self.assertLessEqual(request['request_date'], unit['issued_at'])
                self.assertLess(unit['issued_at'], unit['expiration_date'])
                fulfilled[unit['request_id']] += 1
            else:
                self.assertIsNone(unit['request_id'])
                self.assertEqual(unit['unit_status'], 'Expired' if unit['expiration_date'] <= now else 'Available')
        for request in requests.values():
            self.assertEqual(request['units_fulfilled'], fulfilled[request['request_id']])
            self.assertLessEqual(request['units_fulfilled'], request['units_requested'])
            expected = ('Fulfilled' if request['units_fulfilled'] == request['units_requested']
                        else 'Partially Fulfilled' if request['units_fulfilled'] else 'Pending')
            self.assertEqual(request['request_status'], expected)
        self.assertEqual({r['unit_status'] for r in data['donations']}, {'Available', 'Issued', 'Expired'})
        self.assertTrue(any(r['request_status'] == 'Partially Fulfilled' for r in requests.values()))


@unittest.skipUnless(os.getenv('RUN_MONGO_TESTS') == '1', 'Set RUN_MONGO_TESTS=1 to use real MongoDB.')
class MongoTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database_name = 'blood_bank_test_' + uuid4().hex
        cls.client, cls.db = connect(cls.database_name)
        create_indexes(cls.db)

    @classmethod
    def tearDownClass(cls):
        assert cls.db.name == cls.database_name and cls.database_name.startswith('blood_bank_test_')
        cls.client.drop_database(cls.database_name)
        cls.client.close()

    def setUp(self):
        for name in ('donors', 'donations', 'blood_requests'):
            self.db[name].delete_many({})
        self.now = utcnow()

    def donor(self, group='O+', donor_id='DNR-TEST'):
        return ops.add_donor(self.db, 'Synthetic Student', 25, 'Other', group, 'Pune',
                             '0000000001', self.now - timedelta(days=500), donor_id)

    def unit(self, unit_id, days_old=1, donor_id='DNR-TEST'):
        return ops.add_donation(self.db, donor_id, self.now - timedelta(days=days_old), unit_id)

    def request(self, quantity=2, group='O+', request_id='REQ-TEST'):
        return ops.add_request(self.db, 'Synthetic Hospital', group, quantity, self.now, request_id)

    def test_connection_and_empty_dashboard(self):
        self.assertEqual(self.client.admin.command('ping')['ok'], 1)
        result = analytics.dashboard(self.db)
        self.assertTrue(all(value == 0 for value in result['kpis'].values()))
        self.assertEqual(len(result['low_stock']), 8)

    def test_donor_search_validation_and_duplicates(self):
        self.donor()
        with self.assertRaises(ValueError):
            self.donor()
        self.assertEqual(ops.list_donors(self.db, 'O+', 'pun')[0], 1)
        self.assertEqual(ops.list_donors(self.db, city='.*')[0], 0)
        with self.assertRaises(ValueError):
            ops.add_donor(self.db, 'Valid Name', 25, 'Other', 'O+', 'Pune', '123', self.now)

    def test_donation_relationship_and_ids(self):
        with self.assertRaises(ValueError):
            self.unit('UNT-MISSING')
        self.donor('AB-')
        unit = self.unit('UNT-TEST')
        self.assertEqual(unit['blood_group'], 'AB-')
        with self.assertRaises(ValueError):
            self.unit('UNT-TEST')
        with self.assertRaises(ValueError):
            ops.add_donation(self.db, 'DNR-TEST', self.now, 'UNT-NEW', unit['donation_id'])
        with self.assertRaises(ValueError):
            self.unit('UNT-OLD', 600)

    def test_fefo_partial_fulfillment_and_expiry(self):
        self.donor()
        self.unit('UNT-LATER', 1)
        self.unit('UNT-FIRST', SHELF_LIFE_DAYS - 1)
        expired = self.unit('UNT-EXPIRED', SHELF_LIFE_DAYS + 1)
        # Even a stale Available flag must not make an expired unit issuable.
        self.db.donations.update_one({'_id': expired['_id']}, {'$set': {'unit_status': 'Available'}})
        self.request(3)
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST', 1), ['UNT-FIRST'])
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), ['UNT-LATER'])
        row = self.db.blood_requests.find_one({'request_id': 'REQ-TEST'})
        self.assertEqual((row['units_fulfilled'], row['request_status']), (2, 'Partially Fulfilled'))
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), [])
        self.assertEqual(self.db.donations.count_documents(ops.available_filter()), 0)
        self.assertEqual(ops.mark_expired(self.db), 1)
        self.assertEqual(analytics.dashboard(self.db)['kpis']['Expired Blood Units'], 1)

    def test_exact_group_and_complete_request(self):
        self.donor('O-')
        self.unit('UNT-TEST')
        self.request(1, 'O+')
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), [])
        self.request(1, 'O-', 'REQ-MATCH')
        self.assertEqual(len(ops.fulfill_request(self.db, 'REQ-MATCH')), 1)
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-MATCH'), [])
        self.assertEqual(self.db.blood_requests.find_one({'request_id': 'REQ-MATCH'})['request_status'], 'Fulfilled')

    def test_expiry_boundaries_and_issued_history(self):
        self.donor()
        for name in ('UNT-NOW', 'UNT-NEAR', 'UNT-LATER', 'UNT-ISSUED'):
            self.unit(name)
        for name, delta in [('UNT-NOW', 0), ('UNT-NEAR', 7), ('UNT-LATER', 8), ('UNT-ISSUED', -1)]:
            self.db.donations.update_one({'unit_id': name}, {'$set': {'expiration_date': self.now + timedelta(days=delta)}})
        self.db.donations.update_one({'unit_id': 'UNT-ISSUED'}, {'$set': {'unit_status': 'Issued'}})
        result = analytics.dashboard(self.db, now=self.now)
        self.assertEqual(result['kpis']['Expired Blood Units'], 1)
        self.assertEqual(result['kpis']['Blood Units Near Expiration'], 1)
        self.assertEqual(result['kpis']['Currently Available Blood Units'], 2)
        self.assertEqual(sum(row['count'] for row in result['inventory_status']), 4)

    def test_interrupted_summary_recovers_without_overissue(self):
        self.donor()
        self.unit('UNT-ONE')
        self.unit('UNT-TWO')
        self.request(1)
        with patch('operations.reconcile_requests', side_effect=RuntimeError('simulated interruption')):
            with self.assertRaises(RuntimeError):
                ops.fulfill_request(self.db, 'REQ-TEST')
        self.assertEqual(self.db.blood_requests.find_one({})['units_fulfilled'], 0)
        derived = list(self.db.blood_requests.aggregate(ops.request_pipeline()))[0]
        self.assertEqual(derived['units_fulfilled'], 1)
        self.assertEqual(ops.fulfill_request(self.db, 'REQ-TEST'), [])
        self.assertEqual(self.db.blood_requests.find_one({})['units_fulfilled'], 1)
        self.assertEqual(self.db.donations.count_documents(ops.available_filter()), 1)

    def test_two_sessions_cannot_overfulfill(self):
        self.donor()
        for i in range(5):
            self.unit(f'UNT-{i:03d}')
        self.request(2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: ops.fulfill_request(self.db, 'REQ-TEST'), range(2)))
        self.assertEqual(sum(len(rows) for rows in results), 2)
        self.assertEqual(self.db.donations.count_documents(ops.available_filter()), 3)

    def test_aggregation_and_explain(self):
        insert_dataset(self.db, generate_dataset(1000))
        result = analytics.dashboard(self.db)
        self.assertEqual(result['kpis']['Total Donations'], 1000)
        self.assertEqual(sum(row['count'] for row in result['monthly_donations']), 1000)
        self.assertEqual(sum(row['count'] for row in result['inventory_status']), 1000)
        expected = sum(row['units_requested'] for row in self.db.blood_requests.find({}))
        self.assertEqual(sum(row['count'] for row in result['requested_by_group']), expected)
        filtered = analytics.dashboard(self.db, start=self.now.date(), end=self.now.date())
        self.assertEqual(filtered['kpis'], result['kpis'])
        explanation = analytics.explain_inventory(self.db)
        for path in explanation.values():
            self.assertIsInstance(path['statistics']['totalDocsExamined'], int)
        self.assertLessEqual(explanation['indexed']['statistics']['totalDocsExamined'],
                             explanation['collection_scan']['statistics']['totalDocsExamined'])

    def test_stock_threshold(self):
        self.donor()
        for i in range(LOW_STOCK_THRESHOLD):
            self.unit(f'UNT-{i:03d}')
        self.assertNotIn('O+', [row['blood_group'] for row in analytics.dashboard(self.db)['low_stock']])
        self.request(1)
        ops.fulfill_request(self.db, 'REQ-TEST')
        self.assertIn('O+', [row['blood_group'] for row in analytics.dashboard(self.db)['low_stock']])

    def test_streamlit_pages_empty_and_seeded(self):
        import streamlit as st
        from streamlit.testing.v1 import AppTest
        for seeded in (False, True):
            if seeded:
                insert_dataset(self.db, generate_dataset(1000))
            st.cache_resource.clear()
            with patch('database.connect', return_value=(self.client, self.db)):
                app = AppTest.from_file('app.py', default_timeout=30).run()
                self.assertEqual(len(app.exception), 0)
                for page in ['Donor Management', 'Blood Inventory', 'Blood Requests', 'Analytics', 'Dashboard']:
                    app.sidebar.radio[0].set_value(page).run()
                    self.assertEqual(len(app.exception), 0, page)
                    self.assertEqual(len(app.error), 0, page)
        st.cache_resource.clear()

    def test_streamlit_forms_end_to_end(self):
        import streamlit as st
        from streamlit.testing.v1 import AppTest
        st.cache_resource.clear()
        with patch('database.connect', return_value=(self.client, self.db)):
            app = AppTest.from_file('app.py', default_timeout=30).run()
            def field(elements, label):
                return next(element for element in elements if element.label == label)
            app.sidebar.radio[0].set_value('Donor Management').run()
            field(app.text_input, 'Donor ID (blank = automatic)').set_value('DNR-UI')
            field(app.text_input, 'Name').set_value('Synthetic UI Donor')
            field(app.text_input, 'City').set_value('Pune')
            field(app.button, 'Register donor').click().run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(self.db.donors.count_documents({'donor_id': 'DNR-UI'}), 1)
            app.sidebar.radio[0].set_value('Blood Inventory').run()
            field(app.text_input, 'Existing donor ID').set_value('DNR-UI')
            field(app.text_input, 'Unit ID (blank = automatic)').set_value('UNT-UI')
            field(app.button, 'Record donation').click().run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(self.db.donations.count_documents(ops.available_filter()), 1)
            app.sidebar.radio[0].set_value('Blood Requests').run()
            field(app.text_input, 'Request ID (blank = automatic)').set_value('REQ-UI')
            field(app.text_input, 'Synthetic hospital name').set_value('Synthetic UI Hospital')
            field(app.button, 'Add request').click().run()
            field(app.text_input, 'Request ID to fulfill').set_value('REQ-UI')
            field(app.button, 'Issue available units').click().run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(len(app.error), 0)
            self.assertEqual(self.db.blood_requests.find_one({'request_id': 'REQ-UI'})['request_status'], 'Fulfilled')
            self.assertEqual(self.db.donations.count_documents(ops.available_filter()), 0)
            app.sidebar.radio[0].set_value('Dashboard').run()
            field(app.checkbox, 'Filter historical charts by date').check().run()
            self.assertEqual(len(app.exception), 0)
            app.sidebar.radio[0].set_value('Analytics').run()
            field(app.button, 'Run explain comparison').click().run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(len(app.error), 0)
        st.cache_resource.clear()


if __name__ == '__main__':
    unittest.main(verbosity=2)
