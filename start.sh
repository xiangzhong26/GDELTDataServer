#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"
if [[ ! -x .venv/bin/python ]]; then
  python3 -m venv .venv
fi
.venv/bin/python -m pip install -e .
if [[ ! -f config.local.json ]]; then
  cp config.example.json config.local.json
fi
exec .venv/bin/python -m gdelt_server --config config.local.json serve
