"""Run inside the sample UI container: real HTTP + Streamlit AppTest, no mocked fetch."""
import json,os
import requests
from streamlit.testing.v1 import AppTest

def main():
    base=os.environ['API_BASE_URL'];station=os.environ['STATION_ID'];checks=[]
    for route in ['/health','/ready','/stations']:
        response=requests.get(base+route,timeout=15);assert response.status_code==200,(route,response.status_code)
        checks.append('Actual API '+route)
    response=requests.get(base+'/predictions/'+station,timeout=15);response.raise_for_status();payload=response.json()
    assert payload['station_id']==station and payload['count']==1
    assert payload['predictions'][0]['model_version']=='not-for-deployment'
    assert requests.get(base+'/predictions/'+station+'?limit=0',timeout=15).status_code==422
    hour=payload['predictions'][0]['prediction_hour_ts']
    assert requests.get(base+'/predictions/'+station+'/'+hour,timeout=15).status_code==200
    assert requests.get('http://localhost:8501/_stcore/health',timeout=15).status_code==200
    checks.append('Actual PostgreSQL-backed HTTP prediction and timestamp/limit validation')
    app=AppTest.from_file('/app/app.py').run(timeout=30)
    assert not app.exception,[error.message for error in app.exception]
    assert len(app.metric)==4
    assert any('Validation sample only' in warning.value for warning in app.warning)
    assert len(app.get('plotly_chart'))==2
    app.selectbox[0].select('7 Days').run(timeout=30)
    assert not app.exception and len(app.metric)==4
    checks.append('Live-HTTP Streamlit render: four metrics, two charts, sample warning and lookback filtering')
    print(json.dumps({'complete':True,'station_id':station,'prediction_count':payload['count'],'checks':checks,'browser_automation':False},indent=2))

if __name__=='__main__':main()
