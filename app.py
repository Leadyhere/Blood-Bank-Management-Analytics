"""Run with: python -m streamlit run app.py"""
import json
import math
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo.errors import PyMongoError

st.set_page_config(page_title='Blood Bank Analytics', page_icon='🩸', layout='wide')

try:
    import analytics
    import operations as ops
    from config import (BLOOD_GROUPS, GENDERS, LOW_STOCK_THRESHOLD,
                        NEAR_EXPIRY_DAYS, SHELF_LIFE_DAYS, utcnow)
    from database import connect, create_indexes
except ValueError:
    st.error('Invalid configuration. Shelf life and near-expiry days must be integers from 1 to 3650; '
             'the low-stock threshold must be an integer from 1 to 1000000. Check .env and restart.')
    st.stop()


@st.cache_resource
def database_connection():
    client, db = connect()
    try:
        create_indexes(db)
        ops.reconcile_requests(db)
    except Exception:
        client.close()
        raise
    return client, db


def table(rows):
    if not rows:
        st.info('No matching records. Add records or generate the demonstration dataset.')
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
    except PyMongoError:
        st.error('MongoDB could not complete the operation. Some writes may have succeeded. '
                 'Reconnect and inspect the records before retrying. Issued units remain linked to their request.')


def chart(rows, x, title, kind='bar'):
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
    elif kind == 'pie':
        fig = px.pie(frame, names=x, values='count', hole=.35)
    else:
        fig = px.bar(frame, x=x, y='count', text_auto=True)
    fig.update_layout(height=340, margin=dict(l=10, r=10, t=20, b=10),
                      yaxis_title='Units / records', xaxis_title=x.replace('_', ' ').title())
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
        stats = db.command('dbStats')
        table([{'collection': name, 'documents': db[name].count_documents({}),
                'indexes': len(db[name].index_information())}
               for name in ('donors', 'donations', 'blood_requests')])
        st.caption(f"Logical data size: {stats.get('dataSize', 0):,} bytes; "
                   f"index size: {stats.get('indexSize', 0):,} bytes. Sizes reported by MongoDB.")
        st.subheader('Aggregation pipelines and actual outputs')
        for name, (collection, pipeline) in analytics.pipelines(group=group, start=start, end=end).items():
            with st.expander(name.replace('_', ' ').title()):
                st.caption(f'Collection: {collection}')
                st.code(json.dumps(pipeline, indent=2, default=str), language='json')
                table(list(db[collection].aggregate(pipeline)))
        st.subheader('Index demonstration')
        st.caption('Compares a forced collection scan with the inventory index. No indexes are dropped. '
                   'Milliseconds can be zero for small datasets; compare examined documents too.')
        if st.button('Run explain comparison'):
            explanation = analytics.explain_inventory(db, group or 'O+')
            table([{'access_path': key, **value['statistics']} for key, value in explanation.items()])
            with st.expander('Raw explain output'):
                st.json(json.dumps(explanation, default=str))


def donor_page(db):
    st.title('Donor Management')
    st.metric('Total registered donors', db.donors.count_documents({}))
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
    inventory_pipeline = analytics.grouped_pipeline(ops.available_filter(group=group), 'blood_group')
    table(analytics.all_group_rows(list(db.donations.aggregate(inventory_pipeline)), group))
    status = st.selectbox('Unit status', ['All', 'Available', 'Issued', 'Expired'])
    query = {'blood_group': group} if group else {}
    if status == 'Available':
        query = ops.available_filter(group=group)
    elif status != 'All':
        query['unit_status'] = status
    total = db.donations.count_documents(query)
    skip = offset(total, 'inventory_page')
    table(list(db.donations.find(query).sort([('expiration_date', 1), ('unit_id', 1)]).skip(skip).limit(100)))
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
    pipeline = ops.request_pipeline({'blood_group': group} if group else {})
    if status != 'All':
        pipeline.append({'$match': {'request_status': status}})
    counts = list(db.blood_requests.aggregate(pipeline + [{'$count': 'count'}]))
    total = counts[0]['count'] if counts else 0
    skip = offset(total, 'request_page')
    table(list(db.blood_requests.aggregate(pipeline + [
        {'$sort': {'request_date': -1, 'request_id': 1}}, {'$skip': skip}, {'$limit': 100}])))


def main():
    st.sidebar.title('Blood Bank Analytics')
    page = st.sidebar.radio('Navigation', ['Dashboard', 'Donor Management', 'Blood Inventory',
                                            'Blood Requests', 'Analytics'])
    st.sidebar.caption('College mini project • Synthetic data only')
    st.sidebar.button('Refresh data')
    st.sidebar.info('Run one application process. Educational use only; no clinical decisions.')
    try:
        _, db = database_connection()
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
        st.code('Get-Service MongoDB\n# In an Administrator terminal if stopped:\nStart-Service MongoDB')
    except PyMongoError:
        st.error('MongoDB operation failed. Check the server connection and database permissions, '
                 'then refresh. No credentials are displayed here.')


if __name__ == '__main__':
    main()
