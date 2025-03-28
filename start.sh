#!/bin/bash
echo 'Installing commands'
echo 'alias quant-install="pip install -r python_imports"' >> ~/.bashrc
echo 'alias quant-run="python strategy.py"' >> ~/.bashrc
echo 'alias quant-env="source quant-env/bin/activate"' >> ~/.bashrc
echo 'alias quant-deactivate="deactivate"' >> ~/.bashrc
alias quant-help='list_menu'

source ~/.bashrc
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
function create_virtualenv() {
    echo 'Creating virtual environment'
    python3 -m venv quant-env
    echo 'Virtual environment created!'
}
function list_menu() {
    echo'========================='
    echo 'Run quant-env to activate the virtual environment'
    echo 'Run quant-install to install the required packages'
    echo 'Run quant-run to run the strategy'
    echo 'Run quant-deactivate to deactivate the virtual environment'
    echo 'Run quant-help to see this menu again'
    echo'========================='
}

list_menu()
