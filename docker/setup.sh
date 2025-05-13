#!/bin/bash

# Go to the project directory
PROJECT_DIR=$(pwd)

# Set venv path
VENV_DIR="$PROJECT_DIR/.venv"

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment in $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
fi

# Activate the virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Install dependencies if requirements.txt exists
if [ -f "$PROJECT_DIR/requirements.txt" ]; then
    echo "Installing dependencies from requirements.txt..."
    pip install --upgrade pip
    pip install -r "$PROJECT_DIR/requirements.txt"
else
    echo "No requirements.txt found in $PROJECT_DIR."
fi
