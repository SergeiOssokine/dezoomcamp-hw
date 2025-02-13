#!/bin/bash

echo "Setting up the environment with uv"
uv venv .dezoomcamp
source .dezoomcamp/bin/activate
echo "Installing packages"
uv pip install -r setup/requirements.txt
echo "Installing pre-commit"
pre-commit install
python -c "import requests"
echo "Now activate the environment by running source .dezoomcamp/bin/activate"