"""Real Chromium UI checks against the existing isolated sample services."""
import argparse,json,os,time
from pathlib import Path

def main():
    from playwright.sync_api import sync_playwright,expect
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args()
    output=Path(a.output);output.mkdir(parents=True,exist_ok=False)
    report={'browser':'Chromium','complete':False,'checks':[],'page_errors':[],'console_errors':[],'failed_requests':[],'bad_responses':[]}
    try:
        with sync_playwright() as pw:
            browser=pw.chromium.launch(headless=True)
            page=browser.new_page(viewport={'width':1440,'height':1080})
            page.on('pageerror',lambda error:report['page_errors'].append(str(error)))
            page.on('console',lambda message:report['console_errors'].append(message.text) if message.type=='error' else None)
            page.on('requestfailed',lambda request:report['failed_requests'].append({'url':request.url,'failure':request.failure}))
            page.on('response',lambda response:report['bad_responses'].append({'url':response.url,'status':response.status}) if response.status>=400 else None)
            response=page.request.get('http://127.0.0.1:8011/predictions/6140.05');assert response.status==200
            payload=response.json();assert payload['count']==1
            page.goto('http://127.0.0.1:8503/',wait_until='domcontentloaded')
            expect(page.get_by_text('Validation sample only:',exact=False)).to_be_visible(timeout=45000)
            expect(page.locator('[data-testid="stMetricValue"]')).to_have_count(4)
            expect(page.locator('.js-plotly-plot')).to_have_count(2,timeout=30000)
            expected={0:'Low Demand',1:'Normal Demand',2:'High Demand'}[payload['predictions'][0]['predicted_class']]
            expect(page.locator('[data-testid="stMetricValue"]').first).to_have_text(expected)
            report['checks'].append('Live PostgreSQL-backed API prediction matches rendered prediction')
            combo=page.get_by_role('combobox',name='Evaluation Lookback Horizon')
            for option in ['7 Days','10 Days','14 Days','All History']:
                combo.click();page.get_by_role('option',name=option,exact=True).click()
                expect(page.get_by_text('within: '+option,exact=False)).to_be_visible()
                expect(page.locator('.js-plotly-plot')).to_have_count(2)
            report['checks'].append('All four lookback filters rerender both real Plotly charts')
            plots=page.locator('.js-plotly-plot').evaluate_all('(els)=>els.map(el=>({traces:el.data.length,points:el.data.map(t=>(t.x||[]).length),axis:el.layout.xaxis,modes:el.data.map(t=>t.mode),barWidths:el.data.filter(t=>t.type==="bar").map(t=>t.width)}))')
            assert all(plot['traces']>0 and sum(plot['points'])>0 for plot in plots),plots
            for plot in plots:
                assert plot['axis']['tickmode']=='array' and plot['axis']['ticktext']==['Jan 13 23:00 UTC']
            assert plots[0]['barWidths']==[2700000,2700000]
            assert all('markers' in mode for mode in plots[1]['modes'])
            expect(page.locator('.js-plotly-plot').nth(1).locator('.scatterlayer .point')).to_have_count(2)
            report['plots']=plots
            page.screenshot(path=str(output/'desktop.png'),full_page=True)
            page.locator('.js-plotly-plot').nth(1).scroll_into_view_if_needed()
            page.screenshot(path=str(output/'charts.png'),full_page=True)
            station=page.get_by_label('Start station ID',exact=True)
            station.fill('unknown-browser-station');station.press('Enter')
            expect(page.get_by_text('No predictions were returned.',exact=False)).to_be_visible()
            station.fill('6140.05');station.press('Enter')
            expect(page.locator('[data-testid="stMetricValue"]')).to_have_count(4)
            report['checks'].append('Unknown station warning and restoring sample station recover correctly')
            page.set_viewport_size({'width':390,'height':844})
            expect(page.locator('[data-testid="stMetricValue"]')).to_have_count(4)
            page.get_by_role('heading',name='Citi Bike Demand Operational Performance Portal').scroll_into_view_if_needed()
            page.screenshot(path=str(output/'mobile.png'),full_page=True)
            report['checks'].append('Desktop and mobile browser screenshots captured')
            assert not report['page_errors'],report['page_errors']
            assert not report['console_errors'],report['console_errors']
            assert not report['bad_responses'],report['bad_responses']
            assert not report['failed_requests'],report['failed_requests']
            report['complete']=True;browser.close()
    except Exception as error:report['error']=repr(error);raise
    finally:(output/'browser.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
