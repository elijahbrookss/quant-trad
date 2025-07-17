#!/bin/bash

set -e

# -------------------------------
# 🔧 Dev Setup Script
# -------------------------------
echo "Initializing development environment..."

# 2. Create virtual environment if missing
if [ ! -d "env" ]; then
  echo "Creating virtual environment..."
  python3 -m venv env || { echo "Failed to create virtual environment. This must be resolved manually."; exit 1; }
else
  echo "Virtual environment already exists."
fi

# 3. Activate virtual environment
source env/bin/activate || { echo "Failed to activate virtual environment. This must be resolved manually."; exit 1; }

# Ensure we're in the virtual environment
if [[ "$VIRTUAL_ENV" == "" ]]; then
  echo "Virtual environment not active. Aborting dependency install."
  exit 1
fi


# 4. Install dependencies
if [ ! -f "requirements.txt" ]; then
  echo "Missing requirements.txt. Please ensure it exists at the project root."
  exit 1
fi

echo "Installing dependencies..."
pip install -r requirements.txt || { echo "pip install failed. Please check the packages in requirements.txt."; exit 1; }

# 5. Load .env (optional)
if [ -f ".env" ]; then
  echo "Loading environment variables from .env"
  export $(grep -v '^#' .env | xargs)
else
  echo ".env file not found. Proceeding without it. You may create one to set flags like DEBUG or ENV."
fi

# 6. Load secrets.env (optional)
if [ -f "secrets.env" ]; then
  echo "🔐 Loading secrets from secrets.env"
  export $(grep -v '^#' secrets.env | xargs)
else
  echo "secrets.env not found. You can create one to define private keys or API credentials."
fi

# 7. Validate PYTHONPATH
if [[ ":$PYTHONPATH:" != *":$(pwd)/src:"* ]]; then
  export PYTHONPATH=$(pwd)
  echo "PYTHONPATH set to: $PYTHONPATH"
fi

# 8. Finish
echo "Dev environment ready. Suggested next step:"
echo "make setup"
echo "To run the application, use:"
echo "make run"
echo "To run tests, use:"
echo "make test"