"""Run with: python -m streamlit run app.py"""
import json
import math
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title='Blood Bank Analytics', page_icon='🩸', layout='wide')

try:
    import analytics
    import presentation as visuals
    import public_data as source
    import operations as ops
    from config import (BLOOD_GROUPS, GENDERS, LOW_STOCK_THRESHOLD,
                        NEAR_EXPIRY_DAYS, SHELF_LIFE_DAYS, PUBLIC_DATA_DB, utcnow)
    from database import connect, StorageError, clear_query_cache
except ValueError:
    st.error('Invalid configuration. Shelf life and near-expiry days must be integers from 1 to 3650; '
             'the low-stock threshold must be an integer from 1 to 1000000. Check .env and restart.')
    st.stop()


@st.cache_resource
def database_connection():
    db = connect()
    ops.reconcile_requests(db)
    return db


@st.cache_resource
def public_database_connection():
    return connect(PUBLIC_DATA_DB)


def table(rows):
    if not rows:
        st.info('No matching records for this selection.')
        return
    frame = pd.DataFrame(rows).drop(columns=['_id'], errors='ignore')
    st.dataframe(frame, hide_index=True, width='stretch')


def offset(total, key):
    pages = max(1, math.ceil(total / 100))
    # Reset a page that is now out of range after changing a filter.
    if st.session_state.get(key, 1) > pages:
        st.session_state[key] = 1
    page = st.number_input('Page (100 rows per page)', min_value=1, max_value=pages,
                           step=1, key=key)
    st.caption(f'{total:,} matching records • {pages:,} page(s)')
    return (page - 1) * 100


def group_selector(label, key):
    selected = st.selectbox(label, ['All', *BLOOD_GROUPS], key=key)
    return None if selected == 'All' else selected


def write_action(function, *args, success='Saved.', **kwargs):
    try:
        result = function(*args, **kwargs)
        st.session_state['notice'] = success(result) if callable(success) else success
        st.rerun()
    except ValueError as exc:
        st.error(str(exc))
    except (StorageError, OSError):
        st.error('The snapshot operation could not complete. Refresh and inspect the current records before retrying. '
                 'Inventory and request summaries are published together.')


def chart(rows, x, title, kind='bar', ylabel='Units / records'):
    st.subheader(title)
    if not rows or not any(row['count'] for row in rows):
        st.info('No data for this chart and selection.')
        return
    frame = pd.DataFrame(rows)
    if kind == 'line':
        # Fill internal missing months with zero, without inventing observations.
        frame.index = pd.to_datetime(frame[x])
        frame = frame.reindex(pd.date_range(frame.index.min(), frame.index.max(), freq='MS'))
        frame['count'] = frame['count'].fillna(0)
        frame[x] = frame.index.strftime('%Y-%m')
        fig = px.line(frame, x=x, y='count', markers=True)
    elif kind == 'daily':
        fig = px.line(frame, x=x, y='count')
    elif kind == 'pie':
        fig = px.pie(frame, names=x, values='count', hole=.35)
    else:
        fig = px.bar(frame, x=x, y='count', text_auto=True)
    fig = visuals.polish(fig, ylabel=ylabel)
    st.plotly_chart(fig, width='stretch', key=f'chart_{title}')


def alert_tables(db, group=None, detailed=False):
    for near, label in [(True, f'Unissued units expiring within {NEAR_EXPIRY_DAYS} days'),
                        (False, 'Expired unissued units')]:
        total, rows = analytics.expiration_units(db, near, group)
        with st.expander(f'{label} ({total:,})'):
            if detailed:
                skip = offset(total, f'expiry_page_{near}')
                _, rows = analytics.expiration_units(db, near, group, skip=skip)
            elif total > 100:
                st.caption('Showing the first 100. Browse all records on Blood Inventory.')
            table(rows)


def report_page(db, detailed=False):
    st.title('Detailed Analytics' if detailed else 'Blood Bank Dashboard')
    group = group_selector('Blood group', 'report_group')
    apply_dates = st.checkbox('Filter historical charts by date', value=False)
    start = end = None
    if apply_dates:
        selected = st.date_input('Historical date range (UTC, inclusive)',
                                 value=(utcnow().date() - timedelta(days=365), utcnow().date()),
                                 min_value=date(1900, 1, 1), max_value=utcnow().date())
        if len(selected) != 2:
            st.info('Select both a start and an end date.')
            return
        start, end = selected
    st.caption('KPIs, inventory status and alerts show current/all-time values for the selected blood group. '
               'Date range filters donation, request and donor-registration charts only. '
               'Pending requests includes partially fulfilled requests. All dates are UTC.')
    result = analytics.dashboard(db, group, start, end)
    labels = list(result['kpis'].items())
    for row in (labels[:3], labels[3:]):
        for column, (label, value) in zip(st.columns(3), row):
            column.metric(label, f'{value:,}')
    if result['low_stock']:
        message = ', '.join(f"{row['blood_group']}: {row['count']}" for row in result['low_stock'])
        st.warning(f'Low stock (below {LOW_STOCK_THRESHOLD} units): {message}')
    else:
        st.success('Selected blood groups meet the stock threshold.')
    alert_tables(db, group)
    charts = [
        ('available_by_group', 'blood_group', 'Blood Group Inventory', 'bar'),
        ('monthly_donations', 'month', 'Monthly Donation Trends', 'line'),
        ('donation_distribution', 'blood_group', 'Blood Group Distribution', 'pie'),
        ('requested_by_group', 'blood_group', 'Blood Requests by Blood Group', 'bar'),
        ('inventory_status', 'status', 'Inventory Status Distribution', 'pie'),
        ('top_cities', 'city', 'Top Donor Cities', 'bar'),
    ]
    for position in range(0, len(charts), 2):
        for column, (name, x, title, kind) in zip(st.columns(2), charts[position:position + 2]):
            with column:
                chart(result[name], x, title, kind)
    if group:
        st.caption('Only the selected blood group is shown. Select All to compare all eight groups.')
    if detailed:
        chart(result['monthly_requests'], 'month', 'Monthly Requested Units', 'line')
        st.subheader('Dataset statistics (entire database)')
        table(db.stats())
        st.caption('Immutable local Parquet tables; queries execute in Apache Spark SQL.')
        st.subheader('Spark SQL queries and actual outputs')
        for name, sql in analytics.pipelines(group=group, start=start, end=end).items():
            with st.expander(name.replace('_', ' ').title()):
                st.code(sql, language='sql')
                table(result[name])
        st.subheader('Spark execution plan')
        st.caption('Inspect Parquet FileScan, pushed filters and shuffle exchanges. No MongoDB indexes remain.')
        if st.button('Run explain comparison'):
            st.json(analytics.explain_inventory(db, group or 'O+'))



def donor_page(db):
    st.title('Donor Management')
    st.metric('Total registered donors', db.count('donors'))
    with st.expander('Register a synthetic donor'):
        with st.form('donor_form'):
            donor_id = st.text_input('Donor ID (blank = automatic)')
            name = st.text_input('Name')
            age = st.number_input('Age (demonstration range)', 18, 65, 25)
            gender = st.selectbox('Gender', GENDERS)
            group = st.selectbox('Blood group', BLOOD_GROUPS)
            city = st.text_input('City')
            contact = st.text_input('Synthetic contact number', value='0000000001')
            registered = st.date_input('Registration date', value=utcnow().date(),
                                       min_value=date(1900, 1, 1), max_value=utcnow().date())
            if st.form_submit_button('Register donor'):
                write_action(ops.add_donor, db, name, age, gender, group, city, contact,
                             registered, donor_id, success=lambda row: f"Registered {row['donor_id']}.")
    group = group_selector('Search by blood group', 'donor_group')
    city = st.text_input('Search city (contains)')
    total, _ = ops.list_donors(db, group, city, limit=1)
    skip = offset(total, 'donor_page')
    _, rows = ops.list_donors(db, group, city, skip)
    table(rows)


def inventory_page(db):
    st.title('Blood Inventory')
    st.caption(f'One donation = one standardized demonstration red blood cell unit. '
               f'Shelf life: {SHELF_LIFE_DAYS} days. Actual component storage rules vary.')
    with st.expander('Record a donation'):
        with st.form('donation_form'):
            donor = st.text_input('Existing donor ID')
            unit_id = st.text_input('Unit ID (blank = automatic)')
            donation_id = st.text_input('Donation ID (blank = automatic)')
            donated = st.date_input('Donation date', value=utcnow().date(),
                                    min_value=date(1900, 1, 1), max_value=utcnow().date())
            if st.form_submit_button('Record donation'):
                write_action(ops.add_donation, db, donor, donated, unit_id, donation_id,
                             success=lambda row: f"Recorded {row['unit_id']} ({row['unit_status']}).")
    group = group_selector('Filter blood group', 'inventory_group')
    sql = analytics.pipelines(group=group)['available_by_group']
    table(analytics.all_group_rows(db.query(sql), group))
    status = st.selectbox('Unit status', ['All', 'Available', 'Issued', 'Expired'])
    total, _ = ops.list_units(db, group, status, limit=1)
    skip = offset(total, 'inventory_page')
    _, rows = ops.list_units(db, group, status, skip)
    table(rows)
    st.caption('Expiration statuses refresh on each interaction. Use Refresh data in the sidebar '
               'after leaving the app idle. Units are issued only through Blood Requests.')
    alert_tables(db, group, detailed=True)


def requests_page(db):
    st.title('Blood Requests')
    with st.expander('Add a hospital request'):
        with st.form('request_form'):
            request_id = st.text_input('Request ID (blank = automatic)')
            hospital = st.text_input('Synthetic hospital name')
            group = st.selectbox('Requested blood group', BLOOD_GROUPS)
            quantity = st.number_input('Units requested', 1, 10000, 1)
            requested = st.date_input('Request date', value=utcnow().date(),
                                      min_value=date(1900, 1, 1), max_value=utcnow().date())
            if st.form_submit_button('Add request'):
                write_action(ops.add_request, db, hospital, group, quantity, requested, request_id,
                             success=lambda row: f"Created {row['request_id']}.")
    with st.form('fulfillment_form'):
        st.subheader('Fulfill a request')
        request_id = st.text_input('Request ID to fulfill')
        limit = st.number_input('Maximum units to issue this time', 1, 10000, 10)
        st.caption('Exact blood-group match; earliest expiration first. Outstanding demand and '
                   'available inventory cap the quantity automatically.')
        if st.form_submit_button('Issue available units'):
            write_action(ops.fulfill_request, db, request_id, limit,
                         success=lambda units: (f'Issued {len(units)} unit(s). '
                                                'The table shows the updated request status.'))
    group = group_selector('Filter requested blood group', 'request_group')
    status = st.selectbox('Request status', ['All', 'Pending', 'Partially Fulfilled', 'Fulfilled'])
    total, _ = ops.list_requests(db, group, status, limit=1)
    skip = offset(total, 'request_page')
    _, rows = ops.list_requests(db, group, status, skip)
    table(rows)



@st.cache_data(show_spinner=False, ttl=900, max_entries=32)
def cached_public_report(root, version, start, end, state, group, _db):
    return source.public_report(_db, start, end, state, group)


def public_report_page(db, detailed=False):
    st.caption('PUBLIC DATA  /  MALAYSIA  /  APACHE SPARK')
    st.title('Explore donation analytics' if detailed else 'Blood donation insights')
    st.write('Understand donation activity across time, collection states and ABO blood groups.')
    snapshot = db.manifest()
    metadata = snapshot['metadata']
    if not db.count(source.COLLECTION, snapshot):
        st.info('Import the included official CSV to display the real-data dashboard.')
        st.code('python public_data.py')
        return
    if metadata.get('date_min') and metadata.get('date_max'):
        first, last = date.fromisoformat(metadata['date_min']), date.fromisoformat(metadata['date_max'])
    else:
        bounds = db.query('SELECT MIN(date) AS first, MAX(date) AS last FROM daily_donations', snapshot)[0]
        first, last = bounds['first'].date(), bounds['last'].date()
    st.caption(f'Source: National Blood Centre / Ministry of Health Malaysia · '
               f'{db.count(source.COLLECTION, snapshot):,} source records · '
               f'{first:%d %b %Y} – {last:%d %b %Y} · CC BY 4.0')
    with st.container(border=True):
        dates, states, groups = st.columns([2, 1.3, 1])
        with dates:
            selected = st.date_input('Dataset date range (inclusive)', value=(first, last),
                                     min_value=first, max_value=last)
        with states:
            state_choice = st.selectbox('Collection state', ['All', *source.STATES])
        with groups:
            group_choice = st.selectbox('ABO blood group (Rh not supplied)', ['All', 'A', 'B', 'AB', 'O'])
    if len(selected) != 2:
        st.info('Select both dates.')
        return
    start, end = selected
    state = None if state_choice == 'All' else state_choice
    group = group_choice.lower()
    with st.spinner('Preparing your donation report with Spark SQL...'):
        result = cached_public_report(str(db.root), snapshot['version'], start, end, state, group, db)
    avg = result['total'] / result['days'] if result['days'] else 0
    metrics = [('Recorded donations', f"{result['total']:,}", 'Published recorded counts for your selection.'),
               ('Average / observed day', f'{avg:,.1f}', 'Total divided by calendar dates represented in the selection.'),
               ('Collection states', result['states_count'], 'Collection location; this is not donor residence.'),
               ('Days represented', f"{result['days']:,}", 'Distinct dates in the selected source observations.')]
    for column, (label, value, help_text) in zip(st.columns(4), metrics):
        with column:
            with st.container(border=True):
                st.metric(label, value, help=help_text)
    scope = f'{state_choice} states' if state is None else state
    st.caption(f'{scope} · {"All ABO groups" if group == "all" else group_choice + " group"} · '
               f'{start:%d %b %Y} – {end:%d %b %Y}. Counts describe donations, not unique donors or available stock.')
    if group == 'all' and result['total_minus_abo']:
        st.info(f"Source quality: published totals differ from the ABO breakdown by "
                f"{result['total_minus_abo']:,} donations in this selection. Both are preserved.")
    if not result['rows']:
        st.info('No observations match this selection. Adjust the date range, state or group.')
        return
    overview, patterns, evidence = st.tabs(['Overview', 'Trends & patterns', 'Source & data'])
    with overview:
        main_chart, mix = st.columns([2.1, 1])
        with main_chart:
            with st.container(border=True):
                st.subheader('Monthly donation trend')
                change = visuals.complete_month_change(result['monthly'], start, end)
                if change:
                    st.caption(f'{change[0]}: {change[1]:+.1%} versus the same complete month one year earlier.')
                st.caption('Gold markers identify incomplete boundary months; compare complete periods.')
                st.plotly_chart(visuals.monthly_figure(result['monthly'], start, end), width='stretch', key='public_monthly')
        with mix:
            with st.container(border=True):
                st.subheader('ABO distribution')
                st.caption(f"{result['abo_total']:,} categorized donations")
                st.plotly_chart(visuals.group_figure(result['groups']), width='stretch', key='public_abo')
        insights = visuals.findings(result)
        for column, (label, value, explanation) in zip(st.columns(3), insights):
            with column:
                with st.container(border=True):
                    st.caption(label.upper())
                    st.subheader(value)
                    st.caption(explanation)
        with st.container(border=True):
            st.subheader('Donation activity by collection state')
            st.caption('Ranked recorded totals. Coverage varies by collection site; these are not population rates.')
            st.plotly_chart(visuals.state_figure(result['states']), width='stretch', key='public_states')
    with patterns:
        with st.container(border=True):
            st.subheader('Daily activity and seven-day average')
            st.caption('The average requires seven consecutive observed calendar days; missing dates are not filled with zero.')
            st.plotly_chart(visuals.daily_figure(result['daily']), width='stretch', key='public_daily')
        annual, weekday = st.columns(2)
        with annual:
            with st.container(border=True):
                st.subheader('Yearly donation totals')
                st.caption('Gold bars identify incomplete boundary years. Compare equivalent date ranges.')
                st.plotly_chart(visuals.yearly_figure(result['yearly'], start, end), width='stretch', key='public_yearly')
        with weekday:
            with st.container(border=True):
                st.subheader('Weekday pattern')
                measure = st.radio('Weekday measure', ['Average per observed day', 'Total donations'],
                                   horizontal=True)
                st.plotly_chart(visuals.weekday_figure(result['weekdays'], result['daily'],
                    average=measure == 'Average per observed day'), width='stretch', key='public_weekdays')
                st.caption('Monday–Sunday. Averages account for how many observed dates fall on each weekday.')
    with evidence:
        st.subheader('A traceable public dataset')
        st.markdown(f'[{source.SOURCE_TITLE}]({source.CATALOG_URL}) · '
                    f'[CC BY 4.0 license]({source.LICENSE_URL})')
        st.write('Published by the National Blood Centre / Ministry of Health Malaysia through data.gov.my. '
                 'The original CSV is preserved. Spark SQL aggregates its recorded counts.')
        st.caption(f"{result['rows']:,} matching date/state/group observations · "
                   f"{result['abo_total']:,} ABO breakdown donations")
        st.write('Overall totals select published “all” subtotal rows. The ABO distribution selects only '
                 'A, B, AB and O; the two sets are never added together.')
        with st.expander('Coverage and interpretation'):
            st.write('The publisher covers selected BBISv2 sites, not every donation nationwide. '
                     'Collection state is not donor residence. Annual boundary periods can be incomplete.')
            st.write('Rh factor, individual donors, hospital requests, expiry and current stock are not supplied. '
                     'The management pages use separate synthetic demonstration data.')
            st.caption('State comparisons are descriptive, not population-adjusted donation rates or causal findings.')
        exports = {'Monthly donations': ('monthly', 'month'),
                   'State totals': ('states', 'state'), 'ABO distribution': ('groups', 'blood_group'),
                   'Daily donations': ('daily', 'day')}
        export_choice = st.selectbox('Export selected summary', list(exports))
        export_key, _ = exports[export_choice]
        export_frame = pd.DataFrame(result[export_key]).assign(
            selection_start=start.isoformat(), selection_end=end.isoformat(),
            selection_state=state_choice, selection_group=group_choice)
        st.download_button('Download summary CSV', export_frame.to_csv(index=False).encode('utf-8-sig'),
                           file_name=f'blood_donations_{export_key}.csv', mime='text/csv')
        st.caption('Export follows the filters above. Attribution: Ministry of Health Malaysia / data.gov.my, CC BY 4.0.')
        if detailed:
            st.subheader('Source observations')
            skip = offset(result['rows'], 'public_rows_page')
            query = source.public_filter(start, end, state, group)
            _, rows = ops.list_records(db, source.COLLECTION, query, 'date, state, blood_type', skip)
            table(rows)
            st.caption(f"Source SHA-256: {metadata.get('sha256', 'not recorded')}")
            for name, sql in source.public_pipelines(start, end, state, group).items():
                with st.expander(name.title() + ' · Spark SQL'):
                    st.code(sql, language='sql')
                    table(result[name])
            if st.button('Explain public dataset query'):
                explanation = source.explain_public(db, start, end, state, group)
                st.caption(f"Observed query time: {explanation['seconds']:.3f}s. This is one run, not a benchmark.")
                st.code(explanation['physical_plan'], language='text')
    st.caption('Educational descriptive analytics · Real public aggregates · Inventory workflows use synthetic data.')


def main():
    st.sidebar.title('Blood Bank Analytics')
    page = st.sidebar.radio('Navigation', ['Dashboard', 'Donor Management', 'Blood Inventory',
                                            'Blood Requests', 'Analytics'])
    mode = st.sidebar.selectbox('Analytics data source', ['Public dataset (real)', 'Inventory demo (simulated)'])
    st.sidebar.caption('Apache Spark SQL + local Parquet storage')
    if st.sidebar.button('Refresh data'):
        st.cache_data.clear()
        clear_query_cache()
    st.sidebar.caption('Real public analytics, with a separate inventory demonstration.')
    with st.sidebar.expander('About this project'):
        st.caption('Apache Spark SQL · Parquet · Streamlit. Educational use. One local application process.')
    try:
        if page in ('Dashboard', 'Analytics') and mode == 'Public dataset (real)':
            public_db = public_database_connection()
            public_report_page(public_db, detailed=page == 'Analytics')
            return
        st.info('SIMULATED management data. These donors, units and requests are not from the public dataset.')
        db = database_connection()
        ops.mark_expired(db)
        if 'notice' in st.session_state:
            st.success(st.session_state.pop('notice'))
        if page == 'Dashboard':
            report_page(db)
        elif page == 'Donor Management':
            donor_page(db)
        elif page == 'Blood Inventory':
            inventory_page(db)
        elif page == 'Blood Requests':
            requests_page(db)
        else:
            report_page(db, detailed=True)
    except ConnectionError as exc:
        st.error(str(exc))
        st.code('python database.py')
    except (StorageError, OSError):
        st.error('Spark/storage operation failed. Check Java 21, the project data folder and terminal diagnostics, '
                 'then refresh. No credentials are displayed here.')


if __name__ == '__main__':
    main()
