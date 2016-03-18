#!/bin/bash
set -ex
export LC_ALL=en_US.UTF-8

if [ "$DJANGO_VER" = "" ]; then
    DJANGO_VER=1.7
fi

python_exe=python
if [ "$PYTHONHOME" != "" ]; then
    python_exe=$PYTHONHOME/bin/python
fi
    
virtualenv --no-site-packages --python=$python_exe env
. env/bin/activate

django_branch=stable/${DJANGO_VER}.x

if [ ! -d env/src/django ]; then
    git clone https://github.com/denisenkom/django.git -b $django_branch env/src/django
fi
pushd env/src/django
git pull
popd
python env/bin/pip install -e env/src/django

BACKEND=${BACKEND-sqlserver.pytds}
if [ "$BACKEND" = "sqlserver.pytds" ]; then
    python env/bin/pip install -e git+git://github.com/denisenkom/pytds.git#egg=pytds
fi
if [ "$BACKEND" = "sqlserver.pymssql" ]; then
    python env/bin/pip install cython hg+https://denisenkom@code.google.com/r/denisenkom-pymssql/
fi
python env/bin/pip install pytz==2013d

python env/bin/pip install -e .

export COMPUTERNAME=$HOST
python tests/runtests.py --noinput --settings=test_mssql
