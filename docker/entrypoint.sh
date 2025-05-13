#!/bin/bash
set -e

VENV_PATH="/workspace/sp500/.venv"
REQ_FILE="/workspace/sp500/requirements.txt"
 
if [ ! -d "$VENV_PATH" ]; then
  echo "Creating virtual environment at $VENV_PATH..."
  python3 -m venv "$VENV_PATH"
  source "$VENV_PATH/bin/activate"
  pip install --upgrade pip
  pip install -r "$REQ_FILE"
else
  echo "Virtual environment already exists. Activating..."
  source "$VENV_PATH/bin/activate"
fi

# Start SSH daemon
exec /usr/sbin/sshd -D
