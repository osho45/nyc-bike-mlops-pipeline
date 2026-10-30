#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -eq 0 ]; then echo 'Usage: run_all_months.sh YYYY-MM [YYYY-MM ...]'; exit 1; fi
for month in "$@"; do ./drop_month.sh "${month%-*}" "${month#*-}"; done
