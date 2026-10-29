"""Readable charts and descriptive findings from actual Spark report results."""
from calendar import monthrange
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

RED = '#B42335'
TEAL = '#147D73'
INK = '#253247'
MUTED = '#758196'
COLORS = {'A': RED, 'B': TEAL, 'AB': '#B88622', 'O': '#5578B1'}


def polish(fig, height=340, ylabel='Recorded donations'):
    fig.update_layout(template='plotly_white', height=height,
        font=dict(family='Arial, sans-serif', size=12, color=INK),
        paper_bgcolor='white', plot_bgcolor='white',
        margin=dict(l=12, r=18, t=24, b=12),
        hoverlabel=dict(bgcolor='white'),
        legend=dict(orientation='h', yanchor='bottom', y=1.01, x=0),
        xaxis_title=None, yaxis_title=ylabel)
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor='#EDF0F5', zeroline=False, tickformat=',~s')
    return fig


def monthly_figure(rows, start=None, end=None):
    frame = pd.DataFrame(rows)
    fig = px.area(frame, x='month', y='count', color_discrete_sequence=[RED])
    fig.update_traces(line=dict(width=2.5), fillcolor='rgba(180,35,53,0.08)',
                      hovertemplate='%{x}<br><b>%{y:,.0f}</b> recorded donations<extra></extra>')
    fig.update_layout(hovermode='x unified')
    if start and end:
        partial = []
        for row in rows:
            year, month = map(int, row['month'].split('-'))
            first = datetime(year, month, 1).date()
            last = datetime(year, month, monthrange(year, month)[1]).date()
            if first < start or last > end:
                partial.append(row)
        if partial:
            fig.add_scatter(x=[row['month'] for row in partial], y=[row['count'] for row in partial],
                mode='markers', name='Partial calendar month',
                marker=dict(color='#B88622', size=10, line=dict(color='white', width=1)),
                hovertemplate='%{x} (partial month)<br>%{y:,.0f} donations<extra></extra>')
    return polish(fig, 370)


def group_figure(rows):
    frame = pd.DataFrame(rows)
    fig = px.pie(frame, names='blood_group', values='count', color='blood_group',
                 color_discrete_map=COLORS, hole=.67)
    fig.update_traces(textposition='outside', textinfo='label+percent', sort=False,
                      hovertemplate='Group %{label}<br>%{value:,.0f} donations<br>%{percent}<extra></extra>')
    fig.add_annotation(text='ABO<br>mix', x=.5, y=.5, showarrow=False,
                       font=dict(size=18, color=INK))
    return polish(fig, 370)


def state_figure(rows):
    frame = pd.DataFrame(rows).sort_values('count')
    fig = px.bar(frame, x='count', y='state', orientation='h', text='count',
                 color_discrete_sequence=[TEAL])
    fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside', cliponaxis=False,
                      hovertemplate='%{y}<br><b>%{x:,.0f}</b> donations<extra></extra>')
    fig.update_layout(showlegend=False, xaxis_title='Recorded donations', yaxis_title=None)
    fig = polish(fig, max(260, 32 * len(rows) + 60), ylabel=None)
    fig.update_xaxes(gridcolor='#EDF0F5', showgrid=True, tickformat=',~s')
    fig.update_yaxes(showgrid=False, automargin=True)
    return fig


def daily_figure(rows):
    frame = pd.DataFrame(rows)
    frame['day'] = pd.to_datetime(frame['day'])
    frame = frame.set_index('day').asfreq('D')
    fig = go.Figure()
    fig.add_scatter(x=frame.index, y=frame['count'], name='Daily count',
                    mode='lines', line=dict(color='#CBD2DE', width=1), connectgaps=False)
    rolling = frame['count'].rolling(7, min_periods=7).mean()
    fig.add_scatter(x=frame.index, y=rolling, name='7-day average', mode='lines',
                    line=dict(color=RED, width=2), connectgaps=False)
    fig.update_layout(hovermode='x unified')
    return polish(fig, 360)


def yearly_figure(rows, start=None, end=None):
    frame = pd.DataFrame(rows)
    fig = px.bar(frame, x='year', y='count', color_discrete_sequence=[TEAL])
    fig.update_traces(hovertemplate='%{x}<br>%{y:,.0f} donations<extra></extra>')
    if start and end:
        colors = ['#B88622' if datetime(int(row['year']), 1, 1).date() < start
                  or datetime(int(row['year']), 12, 31).date() > end else TEAL for row in rows]
        fig.update_traces(marker_color=colors)
    return polish(fig)


def weekday_figure(rows, daily, average=True):
    occurrences = {i: 0 for i in range(1, 8)}
    for row in daily:
        occurrences[datetime.strptime(row['day'], '%Y-%m-%d').isoweekday()] += 1
    totals = {row['weekday']: row['count'] for row in rows}
    names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    values = [(totals.get(i, 0) / occurrences[i] if occurrences[i] else None)
              if average else totals.get(i, 0) for i in range(1, 8)]
    fig = px.bar(x=names, y=values, color_discrete_sequence=[RED])
    fig.update_traces(hovertemplate='%{x}<br>%{y:,.1f} donations<extra></extra>')
    return polish(fig, ylabel='Average per observed day' if average else 'Recorded donations')


def findings(result):
    """No predictions: statements follow the current selection exactly."""
    items = []
    if result['states'] and result['total']:
        top = result['states'][0]
        items.append(('Leading collection state', top['state'],
                      f"{top['count']:,} donations · {top['count'] / result['total']:.1%} of selected totals"))
    if result['groups'] and result['abo_total']:
        top = max(result['groups'], key=lambda row: row['count'])
        items.append(('Largest recorded ABO group', top['blood_group'],
                      f"{top['count']:,} donations · {top['count'] / result['abo_total']:.1%} of the ABO breakdown"))
    if result['daily']:
        peak = max(result['daily'], key=lambda row: (row['count'], row['day']))
        items.append(('Highest recorded day', peak['day'], f"{peak['count']:,} donations in the current selection"))
    return items


def complete_month_change(monthly, start, end):
    """Compare only fully included calendar months with a nonzero prior year."""
    values = {row['month']: row['count'] for row in monthly}
    for month in sorted(values, reverse=True):
        year, number = map(int, month.split('-'))
        first = datetime(year, number, 1).date()
        last = datetime(year, number, monthrange(year, number)[1]).date()
        previous = f'{year - 1}-{number:02d}'
        prior_first = datetime(year - 1, number, 1).date()
        if first >= start and last <= end and prior_first >= start and values.get(previous, 0) > 0:
            return month, (values[month] - values[previous]) / values[previous]
    return None
