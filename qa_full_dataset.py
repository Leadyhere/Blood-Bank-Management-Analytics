"""Full-snapshot QA, not synthetic screenshots or invented benchmark results.

Uses actual public data and a temporary COPY of management records for UI checks.
Writes qa_results/full_dataset.json; leaves original operational data unchanged.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from unittest.mock import patch

import streamlit as st
from streamlit.testing.v1 import AppTest
from database import connect, Store, spark_session
import public_data as source
from verify_project import audit_database


def main():
    started = perf_counter()
    public, management = connect('public'), connect()
    frame = source.load_csv()
    manifest = source.describe_csv(frame)
    verification = source.verify_snapshot(public, frame, manifest)
    totals = source.public_report(public)
    assert totals['total'] == manifest['published_total_donations']
    assert totals['abo_total'] == manifest['abo_breakdown_total']
    assert totals['total_minus_abo'] == manifest['published_total_minus_abo']
    audit = audit_database(management)
    assert audit['ok'], audit
    pages = []
    with TemporaryDirectory(prefix='blood_bank_full_ui_test_') as folder:
        demo = Store(folder)
        saved = management.manifest()
        with demo.locked():
            demo.publish({name: management.read_table(name, saved)
                          for name in ('donors', 'donations', 'blood_requests')})
        def selected(namespace='management'):
            return public if namespace == 'public' else demo
        st.cache_resource.clear()
        try:
            with patch('database.connect', side_effect=selected):
                app = AppTest.from_file('app.py', default_timeout=180).run()
                assert not app.exception and not app.error
                assert app.metric[0].value == f"{manifest['published_total_donations']:,}"
                pages.append('Public Dashboard')
                app.sidebar.radio[0].set_value('Analytics').run()
                assert not app.exception and not app.error
                pages.append('Public Analytics')
                app.sidebar.selectbox[0].set_value('Inventory demo (simulated)').run()
                for page in ('Dashboard', 'Donor Management', 'Blood Inventory', 'Blood Requests', 'Analytics'):
                    app.sidebar.radio[0].set_value(page).run()
                    assert not app.exception and not app.error, page
                    pages.append('Simulated ' + page)
        finally:
            st.cache_resource.clear()
    result = {'checked_at_utc': datetime.now(timezone.utc).isoformat(),
              'spark_version': spark_session().version, 'ok': True,
              'source_verification': verification, 'operational_audit': audit,
              'public_totals': {key: totals[key] for key in
                                ('total', 'abo_total', 'total_minus_abo', 'rows', 'days', 'states_count')},
              'ui_pages_passed': pages,
              'explain': source.explain_public(public, group='o'),
              'seconds': round(perf_counter() - started, 3)}
    output = Path('qa_results')
    output.mkdir(exist_ok=True)
    (output / 'full_dataset.json').write_text(json.dumps(result, indent=2, default=str) + '\n', encoding='utf-8')
    print(json.dumps({key: value for key, value in result.items() if key != 'explain'}, indent=2))


if __name__ == '__main__':
    main()
