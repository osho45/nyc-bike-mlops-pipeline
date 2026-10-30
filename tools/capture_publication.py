"""Capture genuine UI against the retained 2024 sample; no fitting or data writes."""
import json,os,socket,subprocess,sys,time
from pathlib import Path
import requests

ROOT=Path(__file__).resolve().parents[1]


def main():
    from playwright.sync_api import sync_playwright,expect
    out=ROOT/'artifacts/publication-review';out.mkdir(parents=True,exist_ok=True)
    images=ROOT/'docs/images';images.mkdir(exist_ok=True)
    for port in [8011,8503]:
        with socket.socket() as probe:
            if probe.connect_ex(('127.0.0.1',port))==0:raise RuntimeError(f'Port {port} already occupied; refuse duplicate service')
    local=dict(line.split('=',1) for line in (ROOT/'.env').read_text().splitlines() if '=' in line and not line.startswith('#'))
    env=os.environ.copy();env.update(POSTGRES_CACHE_HOST='127.0.0.1',POSTGRES_CACHE_PORT='5543',POSTGRES_CACHE_USER='cache_user',POSTGRES_CACHE_DB='citibike_validation_container_sample_03',POSTGRES_CACHE_PASSWORD=local['POSTGRES_CACHE_PASSWORD'],API_BASE_URL='http://127.0.0.1:8011',STATION_ID='6140.05',PLAYWRIGHT_BROWSERS_PATH=os.getenv('PLAYWRIGHT_BROWSERS_PATH',str(ROOT/'artifacts/browser-cache')))
    env.pop('PREDICTION_CACHE_BACKEND',None)
    os.environ['PLAYWRIGHT_BROWSERS_PATH']=env['PLAYWRIGHT_BROWSERS_PATH']
    processes=[];logs=[];report={'scope':'Retained genuine January 2024 smoke prediction; not forecast accuracy or deployment','errors':[],'checks':[],'airflow_screenshot':'Unavailable: Airflow services stopped; no restart requested','default_dashboard':'HTTP 200; default API station list empty'}
    try:
        for name,args in [('api',['-m','uvicorn','main:app','--app-dir','fastapi','--host','127.0.0.1','--port','8011']),('dashboard',['-m','streamlit','run','streamlit/app.py','--server.address','127.0.0.1','--server.port','8503','--server.headless','true','--browser.gatherUsageStats','false'])]:
            log=(out/f'{name}.log').open('w');logs.append(log)
            processes.append(subprocess.Popen([sys.executable,*args],cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0))
        for _ in range(45):
            try:
                response=requests.get('http://127.0.0.1:8011/predictions/6140.05',timeout=2);payload=response.json()
                if response.ok and payload.get('count')==1 and requests.get('http://127.0.0.1:8503/_stcore/health',timeout=2).ok:break
            except (requests.RequestException,ValueError):pass
            time.sleep(1)
        else:raise RuntimeError('Retained sample API/dashboard unavailable; inspect ignored logs')
        assert payload['count']==1 and payload['predictions'][0]['prediction_hour_ts'].startswith('2024-01-13')
        report['sample_prediction_count']=1;report['prediction_hour']=payload['predictions'][0]['prediction_hour_ts']
        with sync_playwright() as pw:
            browser=pw.chromium.launch(headless=True);page=browser.new_page(viewport={'width':1600,'height':1800},device_scale_factor=1)
            page.on('pageerror',lambda error:report['errors'].append(str(error)))
            page.on('console',lambda msg:report['errors'].append(msg.text) if msg.type=='error' else None)
            page.goto('http://127.0.0.1:8503',wait_until='domcontentloaded')
            expect(page.get_by_text('Validation sample only:',exact=False)).to_be_visible(timeout=45000)
            expect(page.locator('.js-plotly-plot')).to_have_count(2,timeout=30000)
            expected={0:'Low Demand',1:'Normal Demand',2:'High Demand'}[payload['predictions'][0]['predicted_class']]
            expect(page.locator('[data-testid="stMetricValue"]').first).to_have_text(expected)
            combo=page.get_by_role('combobox',name='Evaluation Lookback Horizon')
            for option in ['7 Days','10 Days','14 Days','All History']:
                combo.click();page.get_by_role('option',name=option,exact=True).click()
                expect(page.get_by_text('within: '+option,exact=False)).to_be_visible()
            page.screenshot(path=str(images/'streamlit-genuine-2024-sample.png'),full_page=True)
            report['checks'].append('Live sample API agrees with dashboard; all four lookback filters render')
            page.set_viewport_size({'width':390,'height':844})
            expect(page.locator('[data-testid="stMetricValue"]')).to_have_count(4)
            page.screenshot(path=str(images/'streamlit-genuine-2024-sample-mobile.png'),full_page=True)
            page.set_viewport_size({'width':1280,'height':960})
            page.goto('http://127.0.0.1:8011/docs',wait_until='networkidle')
            block=page.locator('.opblock').filter(has=page.get_by_text('/predictions/{station_id}',exact=True))
            block.locator('.opblock-summary').click();block.get_by_role('button',name='Try it out').click()
            block.get_by_placeholder('station_id').fill('6140.05');block.get_by_role('button',name='Execute',exact=True).click()
            expect(block.locator('.responses-inner')).to_contain_text('2024-01-13',timeout=15000)
            block.screenshot(path=str(images/'fastapi-genuine-sample-response.png'))
            report['checks'].append('Swagger Execute returns genuine saved 2024 sample prediction')
            assert not report['errors'],report['errors'];report['complete']=True;browser.close()
    finally:
        for process in processes:
            process.terminate()
            try:process.wait(timeout=10)
            except subprocess.TimeoutExpired:process.kill();process.wait()
        for log in logs:log.close()
        (out/'browser.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
