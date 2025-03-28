#!/bin/bash

function loading_bar() {
    local pid=$1
    local delay=0.1
    local progress=0
    local bar_length=40
    echo -n "["
    while kill -0 $pid 2>/dev/null; do
        progress=$((progress + 1))
        local num_hashes=$((progress * bar_length / 100))
        local num_spaces=$((bar_length - num_hashes))
        printf "\r["
        printf "%0.s#" $(seq 1 $num_hashes)
        printf "%0.s " $(seq 1 $num_spaces)
        printf "] %d%%" $((progress % 101))
        sleep $delay
    done
    printf "\r[########################################] 100%%\n"
}

function create_virtualenv() {
    echo 'Creating virtual environment...'
    python3 -m venv quant-env &
    loading_bar $!
    source quant-env/bin/activate
    echo 'Virtual environment created!'
}
function quant-help() {
    echo 'COMMANDS'
    echo '========================='
    echo 'Run quant-env to activate the virtual environment'
    echo 'Run quant-install to install the required packages'
    echo 'Run quant-run to run the strategy'
    echo 'Run quant-deactivate to deactivate the virtual environment'
    echo 'Run quant-help to see this menu again'
    echo '========================='
}

function set_aliases() {
    touch ~/.bash_aliases
    > ~/.bash_aliases
    echo 'alias quant-install="pip install -r python_imports"' >> ~/.bash_aliases
    echo 'alias quant-run="python3 strategy.py"' >> ~/.bash_aliases
    echo 'alias quant-env="source quant-env/bin/activate"' >> ~/.bash_aliases
    echo 'alias quant-deactivate="deactivate"' >> ~/.bash_aliases    
    source ~/.bash_aliases
}

echo 'Installing commands'
set_aliases &
loading_bar $!
echo 'Commands installed!'

echo 'Would you like to create a virtual environment? (y/n)'
read create_env

if [ "$create_env" == "y" ]; then
    create_virtualenv
else
    echo 'Skipping virtual environment creation'
fi

echo 'Would you like to install the required packages? (y/n)'
read install_packages

if [ "$install_packages" == "y" ]; then
    echo 'Installing required packages'
    pip install -r python_imports
    echo 'Packages installed!'
else
    echo 'Skipping package installation'
fi

quant-help