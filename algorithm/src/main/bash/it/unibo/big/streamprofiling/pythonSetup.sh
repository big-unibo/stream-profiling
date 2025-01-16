#!/bin/sh
# Define the path where you want to create the virtual environment
venv_path="../../../../../../../../myenv"

# Check if the virtual environment already exists
if [ ! -d "$venv_path" ]; then
    # Activate the virtual environment based on the platform
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        python3 -m venv "$venv_path"
        source "$venv_path/bin/activate"
    elif [[ "$OSTYPE" == "msys" ]]; then
        python -m venv "$venv_path"
        source "$venv_path/Scripts/activate"
    fi

    # Install the required packages
    pip install -r ../../../../../../../../algorithm/src/main/python/requirements.txt
else
    echo "Virtual environment already exists at $venv_path"
    # Activate the virtual environment based on the platform
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        source "$venv_path/bin/activate"
    elif [[ "$OSTYPE" == "msys" ]]; then
        source "$venv_path/Scripts/activate"
    fi
fi

