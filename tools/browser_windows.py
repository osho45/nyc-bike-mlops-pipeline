"""Real-browser boundary checks on a labeled synthetic, isolated PostgreSQL fixture."""
import argparse,json
from pathlib import Path
from playwright.sync_api import sync_playwright,expect

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--baseline',action='store_true');a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
    result={'synthetic_fixture':True,'complete':False,'windows':{},'errors':[]}
    try:
        with sync_playwright() as pw:
            browser=pw.chromium.launch(headless=True);page=browser.new_page(viewport={'width':1440,'height':1080})
            page.on('pageerror',lambda error:result['errors'].append(str(error)))
            page.on('console',lambda message:result['errors'].append(message.text) if message.type=='error' else None)
            page.goto('http://127.0.0.1:'+('8505' if a.baseline else '8504')+'/',wait_until='domcontentloaded')
            expect(page.get_by_text('Validation sample only:' if a.baseline else 'Synthetic boundary-test data:',exact=False)).to_be_visible(timeout=45000)
            windows=[('All History',1000,None),('7 Days',169,7),('10 Days',241,10),('14 Days',337,14)] if a.baseline else [('All History',1009,None),('7 Days',168,7),('10 Days',240,10),('14 Days',336,14)]
            for label,count,days in windows:
                combo=page.get_by_role('combobox',name='Evaluation Lookback Horizon');combo.click();page.get_by_role('option',name=label,exact=True).click()
                expect(page.get_by_text(f'Showing {count:,} prediction records',exact=False)).to_be_visible(timeout=30000)
                plots=page.locator('.js-plotly-plot');expect(plots).to_have_count(2)
                page.wait_for_function('(expected)=>Array.from(document.querySelectorAll(".js-plotly-plot")).every(p=>p.data && p.data.every(t=>(t.x||[]).length===expected))',arg=count)
                data=page.request.get('http://127.0.0.1:'+('8014' if a.baseline else '8012')+'/predictions/MULTIWEEK_FIXTURE'+('' if days is None else f'?lookback_days={days}')).json()
                if not a.baseline:assert data['total_count']==count
                result['windows'][label]={'displayed_records':count,'api_total_count':data.get('total_count'),'api_page_count':data['count'],'cutoff_exclusive':data.get('window_start_exclusive')}
            page.screenshot(path=str(out/'fourteen-days.png'),full_page=True)
            page.locator('.js-plotly-plot').nth(1).scroll_into_view_if_needed();page.screenshot(path=str(out/'charts.png'),full_page=True)
            assert not result['errors'];result['complete']=True;browser.close()
    except Exception as error:result['error']=repr(error);raise
    finally:(out/'browser.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result,indent=2))

if __name__=='__main__':main()
