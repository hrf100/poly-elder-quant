#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
[ ! -d .venv ] && python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -q
echo "Starting Polymarket BTC 5m Quant Elder Terminal on http://127.0.0.1:8080 (Default Password: qq111111)"
uvicorn backend.main:app --host 0.0.0.0 --port 8080 --workers 1
