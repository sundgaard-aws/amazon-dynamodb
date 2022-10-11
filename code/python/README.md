# Virtual Environments
https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/

``` ps
# upgrade pip if needed
py -m pip install --upgrade pip

# create virtual python environment
py -m venv .\venv

# activate virtual environment
.\venv\Scripts\Activate.ps1

# list current dependencies
py -m pip list

# install dependencies
py -m pip install -r requirements.txt
```

https://docs.python.org/3/library/venv.html
https://pip.pypa.io/en/latest/user_guide/#requirements-files