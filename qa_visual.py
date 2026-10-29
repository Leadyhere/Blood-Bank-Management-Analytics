"""Optional browser review; requires Playwright as a development tool only.

Start Streamlit on port 8501 first. Captures the real running application and
tests report tabs/filters without modifying donor, unit or request records.
"""
import json
from pathlib import Path
from time import perf_counter
from playwright.sync_api import sync_playwright


def main():
    output = Path('qa_results')
    output.mkdir(exist_ok=True)
    started = perf_counter()
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1440, 'height': 2100}, device_scale_factor=1)
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto('http://127.0.0.1:8501', wait_until='domcontentloaded')
        page.get_by_text('9,896,044', exact=True).wait_for(timeout=180000)
        page.locator('[data-testid="stPlotlyChart"]').first.wait_for(timeout=60000)
        page.screenshot(path=str(output / 'dashboard.png'), full_page=True)
        page.get_by_role('tab', name='Trends & patterns').click()
        page.get_by_text('Daily activity and seven-day average', exact=True).wait_for()
        page.screenshot(path=str(output / 'patterns.png'), full_page=True)
        page.get_by_role('tab', name='Source & data').click()
        page.get_by_text('A traceable public dataset', exact=True).wait_for()
        page.get_by_role('button', name='Download summary CSV').wait_for()
        page.get_by_role('tab', name='Overview', exact=True).click()
        # Native Streamlit combobox: choose Sabah and confirm a real filtered report.
        page.get_by_role('combobox', name='Collection state').click()
        page.get_by_role('option', name='Sabah', exact=True).click()
        page.get_by_text('9,896,044', exact=True).wait_for(state='hidden', timeout=120000)
        page.get_by_role('heading', name='Sabah', exact=True).wait_for(timeout=120000)
        page.get_by_role('heading', name='Monthly donation trend', exact=True).wait_for()
        assert not page.get_by_text('9,896,044', exact=True).count(), 'State filter did not change the total'
        # Check that essential content remains accessible on a narrow screen.
        mobile = browser.new_page(viewport={'width': 390, 'height': 844}, device_scale_factor=1)
        mobile.on('pageerror', lambda error: errors.append(str(error)))
        mobile.goto('http://127.0.0.1:8501', wait_until='domcontentloaded')
        mobile.get_by_text('9,896,044', exact=True).wait_for(timeout=120000)
        heading = mobile.get_by_role('heading', name='Blood donation insights', exact=True)
        heading.wait_for()
        assert heading.bounding_box()['x'] >= 0, 'Mobile heading is clipped'
        mobile.screenshot(path=str(output / 'mobile.png'), full_page=True)
        assert not errors, errors
        result = {'ok': True, 'browser_errors': errors,
                  'checks': ['real total', 'overview charts', 'pattern tab', 'source tab',
                             'CSV download control', 'state filter', 'narrow viewport'],
                  'seconds': round(perf_counter() - started, 3)}
        (output / 'visual_review.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, indent=2))
        browser.close()


if __name__ == '__main__':
    main()
