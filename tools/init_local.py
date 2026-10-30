"""Initialize missing local state, never overwrite existing user settings."""
from pathlib import Path
import base64,secrets
root=Path(__file__).resolve().parents[1];p=root/'.env'
if p.exists():print('Existing .env preserved')
else:
    p.write_text('AIRFLOW_UID=1000\nAIRFLOW_FERNET_KEY='+base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()+'\nAIRFLOW_API_SECRET_KEY='+secrets.token_urlsafe(48)+'\nAIRFLOW_JWT_SECRET='+secrets.token_urlsafe(48)+'\nPOSTGRES_CACHE_PASSWORD='+secrets.token_urlsafe(24)+'\nPOSTGRES_METADATA_PASSWORD='+secrets.token_urlsafe(24)+'\nJUPYTER_TOKEN='+secrets.token_urlsafe(32)+'\n',encoding='utf-8')
    print('Created ignored local .env; values not printed')
for name in ['data/citibike/incoming','data/citibike/warehouse','artifacts','models/citibike','logs','sql']:(root/name).mkdir(parents=True,exist_ok=True)
