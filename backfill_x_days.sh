#!/usr/bin/env bash
set -euo pipefail
# Portable replacement for the old macOS-only date loop. Dry run by default.
python tools/backfill.py "$@"
