#!/bin/bash
# -------------------------
# Common helper script that:
# 1. Sets up Python virtual environment
# 2. Installs dependencies
# 3. Runs a specified Python script with arguments
# 4. Profiles memory utilization of the script, with
#    a. outputs in the repo subdirectory named "memory_profiling"
#    g. outputs for each run stamped with the name of the specified
#       script and execution time.

# Usage:
# ./run_python_venv.sh <command> [args...]
# Example: ./run_python_venv.sh ./sab2edge2node.py A,B,C

# Set strict mode for Bash so that failures in subtasks such as python or pip install
# fail loudly.
set -euo pipefail

# Get the path to the python script, which is the first argument.
PYTHON_SCRIPT="${1:-}"

if [[ -z "$PYTHON_SCRIPT" ]]; then
  echo "Error: No Python script specified."
  echo "Usage: $0 <path_to_python_script> <arguments for python script"
  exit 1
fi

if [[ ! -f "$PYTHON_SCRIPT" ]]; then
  echo "Error: Python script '$PYTHON_SCRIPT' not found."
  exit 1
fi

# Set the virtual environment path.
unset VENV
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
VENV="${SCRIPT_DIR}/venv"

if [[ -d "${VENV}" ]]; then
  echo "*** Using existing Python venv in ${VENV}"
  source "${VENV}/bin/activate"
else
  echo "*** Installing Python venv to ${VENV}"
  python3 -m venv "${VENV}"
  python3 -m pip install --upgrade pip
  source "${VENV}/bin/activate"
  echo "*** Installing required packages..."
  pip install -r requirements.txt
  echo "*** Done installing Python venv"
fi

# Execute the specified python script, passing along just the arguments
# for the script (all after the first, which is the name of the script).

echo "Executing Python script: $PYTHON_SCRIPT with arguments " "${@:2}"

# WRAP SCRIPT WITH MEMORY PROFILING.

# Create output dir (relative to the application directory)
OUTDIR="memory_profiling"
mkdir -p "$OUTDIR"

# Generate a file stamp for the memory profile output file that
# includes the name of the profiled script and the generation time.
STAMP="${PYTHON_SCRIPT%.py}_$(date +%Y%m%d_%H%M%S)"
DATFILE="$OUTDIR/mprofile_${STAMP}.dat"
PLOTFILE="$OUTDIR/mprofile_${STAMP}.png"

# Wrap the execution of the script with memory profiling,
# with profiling outputs in OUTDIR.
mprof run -o "$DATFILE" python3 "$PYTHON_SCRIPT" "${@:2}"

# Attempt to plot memory utilization.
# Guard against psutil.NoSuchProcess if the process
# has already exited before mprof/psutil can query it.
python3 - "$DATFILE" "$PLOTFILE" <<'PYEOF'
import sys
import subprocess
import psutil

datfile, plotfile = sys.argv[1], sys.argv[2]

try:
    subprocess.run(
        ["mprof", "plot", "-o", plotfile, datfile],
        check=True,
    )
except psutil.NoSuchProcess as e:
    print(f"Warning: process no longer exists while plotting ({e}); "
          f"skipping plot generation for this run.", file=sys.stderr)
except subprocess.CalledProcessError as e:
    # In case the underlying mprof CLI call itself raises via a traceback
    # containing NoSuchProcess text rather than the exception object directly
    if "psutil.NoSuchProcess" in str(e):
        print(f"Warning: mprof plot failed due to NoSuchProcess ({e}); "
              f"skipping plot generation.", file=sys.stderr)
    else:
        raise
PYEOF
#mprof plot -o "$PLOTFILE" "$DATFILE"

