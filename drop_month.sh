#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -ne 2 ]; then echo 'Usage: drop_month.sh YEAR MONTH'; exit 1; fi
docker compose exec -T jupyter-notebook python -m jobs.download_raw_data --year "$1" --month "$2" --output data/citibike/incoming
