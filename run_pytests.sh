#!/usr/bin/env bash
# run_pytest.sh
# Script to activate venv and run pytest with useful flags

# Exit immediately if any command fails
set -e

# Activate the virtual environment (safe even if already active)
source .venv/Scripts/activate

# Define local log directory and file
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"   # create if not exists
LOG_FILE="$LOG_DIR/pytest_$(date +'%Y%m%d_%H%M%S').log"

# If an argument is given, run that test file; otherwise run the whole tests/ dir
if [ -n "$1" ]; then
    echo "Running pytest on: $1"
    pytest -s -rs -v --color=yes "$1" | tee "$LOG_FILE"
else
    echo "Running pytest on tests/ directory"
    pytest -s -rs -v --color=yes tests/ | tee "$LOG_FILE"
fi

echo ""
echo "Pytest log saved to: $LOG_FILE"