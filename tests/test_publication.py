import json,re
from pathlib import Path
import xml.etree.ElementTree as ET
import pytest
from tools.init_airflow_auth import initialize


def test_readme_local_links_and_images_resolve():
    text=Path('README.md').read_text(encoding='utf-8')
    links=re.findall(r'!?\[[^\]]*\]\(([^)]+)\)',text)
    assert len(links)>=15
    for link in links:
        if not link.startswith(('http:','https:','#')):assert Path(link.split('#')[0]).is_file(),link
    assert 'reused holdout' in text and 'paused after partial preparation' in text
    assert '125,217,844' in text and '3,363,840' in text


def test_portable_metrics_reconcile_and_keep_cohorts_separate():
    data=json.loads(Path('docs/audit/publication_results.json').read_text())
    totals=data['ingestion']['totals']
    assert totals['source_rows']==totals['silver_rows']+totals['quarantine_rows']
    assert data['forecast_384']['validation']['rows']==3363840
    assert data['forecast_384']['validation']['wape']==pytest.approx(.5389625871364885)
    policy=data['policy_48'];assert policy['reused_holdout'] and policy['rows']==310680
    assert policy['penalty_reduction_percent']==pytest.approx(17.8398923604437)
    assert policy['guard_passes']==189 and policy['guard_count']==240


def test_architecture_svg_is_self_contained_and_has_no_scripts():
    root=ET.parse('docs/images/citibike-architecture.svg').getroot()
    assert root.tag.endswith('svg')
    assert not any(node.tag.endswith('script') for node in root.iter())
    text=' '.join(root.itertext())
    assert 'FastAPI' in text and 'LightGBM' in text and 'NOT deployed' in text


def test_airflow_auth_initializer_preserves_existing_credentials(tmp_path):
    path=tmp_path/'auth/generated.json'
    assert initialize(path)
    password=json.loads(path.read_text())['admin']
    assert len(password)>=24 and password!='admin'
    assert not initialize(path) and json.loads(path.read_text())['admin']==password
    path.write_text('{"admin":"existing-private-value"}')
    assert not initialize(path) and json.loads(path.read_text())['admin']=='existing-private-value'


def test_persistence_never_uses_a_password_fallback(monkeypatch):
    from jobs.persistence import postgres_dsn
    from psycopg2.extensions import parse_dsn
    monkeypatch.delenv('POSTGRES_CACHE_PASSWORD',raising=False)
    assert 'password' not in parse_dsn(postgres_dsn())
    monkeypatch.setenv('POSTGRES_CACHE_PASSWORD','synthetic-test-password')
    assert parse_dsn(postgres_dsn())['password']=='synthetic-test-password'
