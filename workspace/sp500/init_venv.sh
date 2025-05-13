#!/bin/bash
set -e

VENV_PATH="/workspace/sp500/env_sp500"
REQ_FILE="/workspace/sp500/requirements.txt"

echo "[INFO] Creating virtual environment in $VENV_PATH..."
python3 -m venv "$VENV_PATH"

echo "[INFO] Activating virtual environment..."
source "$VENV_PATH/bin/activate"

echo "[INFO] Upgrading pip and installing requirements..."
pip install --upgrade pip
pip install -r "$REQ_FILE"

echo "[SUCCESS] Virtual environment setup complete."
source /workspace/sp500/.env_sp500/bin/activate
