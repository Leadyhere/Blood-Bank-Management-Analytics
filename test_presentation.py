"""Guard analytical interpretation in the simplified real-data dashboard."""
import unittest
import math
from datetime import date
import presentation as view


class PresentationTests(unittest.TestCase):
    def test_partial_month_gets_an_explicit_marker(self):
        rows = [{'month': '2025-01', 'count': 30}, {'month': '2025-02', 'count': 10}]
        figure = view.monthly_figure(rows, date(2025, 1, 1), date(2025, 2, 10))
        self.assertEqual(list(figure.data[1].x), ['2025-02'])
        self.assertEqual(figure.data[1].name, 'Partial calendar month')

    def test_partial_year_gets_a_different_color(self):
        figure = view.yearly_figure([{'year': '2024', 'count': 100}, {'year': '2025', 'count': 10}],
                                   date(2024, 1, 1), date(2025, 2, 10))
        self.assertNotEqual(figure.data[0].marker.color[0], figure.data[0].marker.color[1])

    def test_findings_use_selected_denominators(self):
        report = {'total': 100, 'abo_total': 80,
                  'states': [{'state': 'Johor', 'count': 60}],
                  'groups': [{'blood_group': 'A', 'count': 40}, {'blood_group': 'O', 'count': 40}],
                  'daily': [{'day': '2025-01-01', 'count': 100}]}
        items = view.findings(report)
        self.assertIn('60.0%', items[0][2])
        self.assertIn('50.0%', items[1][2])  # ABO denominator, not published-total denominator

    def test_no_findings_for_empty_selection(self):
        self.assertEqual(view.findings({'total': 0, 'abo_total': 0, 'states': [], 'groups': [], 'daily': []}), [])

    def test_partial_months_not_used_for_comparison(self):
        rows = [{'month': '2024-01', 'count': 100}, {'month': '2025-01', 'count': 200}]
        self.assertIsNone(view.complete_month_change(rows, date(2024, 1, 1), date(2025, 1, 10)))
        self.assertEqual(view.complete_month_change(rows, date(2024, 1, 1), date(2025, 1, 31)), ('2025-01', 1.0))

    def test_zero_baseline_has_no_percentage(self):
        rows = [{'month': '2024-01', 'count': 0}, {'month': '2025-01', 'count': 10}]
        self.assertIsNone(view.complete_month_change(rows, date(2024, 1, 1), date(2025, 1, 31)))

    def test_weekday_average_normalizes_observed_dates(self):
        figure = view.weekday_figure([{'weekday': 1, 'count': 30}],
            [{'day': '2025-01-06', 'count': 10}, {'day': '2025-01-13', 'count': 20}])
        self.assertEqual(figure.data[0].y[0], 15)
        self.assertTrue(math.isnan(figure.data[0].y[1]))

    def test_daily_average_does_not_bridge_missing_dates(self):
        rows = [{'day': f'2025-01-{day:02}', 'count': 10} for day in (1, 2, 3, 5, 6, 7, 8)]
        figure = view.daily_figure(rows)
        self.assertTrue(all(value != value for value in figure.data[1].y))

    def test_daily_average_after_seven_complete_days(self):
        rows = [{'day': f'2025-01-{day:02}', 'count': day} for day in range(1, 8)]
        self.assertEqual(view.daily_figure(rows).data[1].y[-1], 4)


if __name__ == '__main__':
    unittest.main(verbosity=2)
