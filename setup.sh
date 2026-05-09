#!/usr/bin/env bash
# Creates a local Python virtual environment, activates it for this shell session,
# installs project dependencies from requirements.txt, and reports completion.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

PYTHON_CMD="python3"
if ! command -v python3 >/dev/null 2>&1; then
  PYTHON_CMD="python"
fi

"$PYTHON_CMD" -m venv venv

if [[ -f "venv/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "venv/bin/activate"
elif [[ -f "venv/Scripts/activate" ]]; then
  # shellcheck source=/dev/null
  source "venv/Scripts/activate"
else
  echo "error: cannot find virtualenv activate script after creating venv" >&2
  exit 1
fi

python -m pip install --upgrade pip >/dev/null
pip install -r requirements.txt

echo "Success: Python venv 'venv' is ready and all requested packages were installed."
