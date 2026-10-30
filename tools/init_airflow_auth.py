"""Create a random local Airflow password only when no auth file exists."""
import json,secrets
from pathlib import Path


def initialize(path):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    try:
        with path.open('x',encoding='utf-8') as out:json.dump({'admin':secrets.token_urlsafe(24)},out)
    except FileExistsError:return False
    return True


if __name__=='__main__':initialize('/opt/airflow/auth/simple_auth_manager_passwords.json.generated')
