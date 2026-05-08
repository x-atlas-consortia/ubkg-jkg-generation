#!/bin/bash

# Set strict mode for Bash so that failures in subtasks such as python or pip install
# fail loudly.
set -euo pipefail

./run_python_venv.sh jkgen2jkg.py "$@"