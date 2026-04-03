#!/bin/bash
# 2026
# sab2edgenode.sh establishes the Python virtual environment for the UBKG-JKG
# generation framework.

# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.
VENV=./venv

if [[ -d ${VENV} ]] ; then
    echo "*** Using Python venv in ${VENV}"
    source ${VENV}/bin/activate
else
    echo "*** Installing Python venv to ${VENV}"
    python3 -m venv ${VENV}
    source ${VENV}/bin/activate
    python -m pip install --upgrade pip
    echo "*** Installing required packages..."
    pip install -r requirements.txt
    echo "*** Done installing python venv"
fi

# Ensure the virtual environment is activated
if [[ -z "$VIRTUAL_ENV" ]]; then
    echo "Virtual environment is NOT active!"
    exit 1
else
    echo "Virtual environment is active: $VIRTUAL_ENV"
fi

echo "Python binary being used:"
which python

# Ensure pythonjsonlogger is installed
if ! (pip show python-json-logger > /dev/null); then
    echo 'Installing python-json-logger...'
    pip install python-json-logger
fi

echo "Running sab2edgenode.py in venv..."p
python ./sab2edgenode.py "$@"
