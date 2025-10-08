#!/usr/bin/env bash
# run_pytest.sh
# Script to activate venv and run pytest with useful flags

# Exit immediately if any command fails
set -e

# Activate the virtual environment (safe even if already active)
source .venv/Scripts/activate

# If an argument is given, run that test file; otherwise run the whole tests/ dir
if [ -n "$1" ]; then
    pytest -s -rs -v "$1"
else
    pytest -s -rs -v tests/
fi