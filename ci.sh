#!/bin/bash
set -e
export LC_ALL=en_US.UTF-8
virtualenv --no-site-packages --python=$PYTHONHOME/bin/python env
. env/bin/activate

django_branch=stable/${DJANGO_VER}.x

if [ ! -d env/src/django ]; then
    git clone https://github.com/denisenkom/django.git -b $django_branch env/src/django
fi
pushd env/src/django
git pull
popd
python env/bin/pip install -e env/src/django

if [ $BACKEND = sqlserver.pytds ]; then
    python env/bin/pip install -e git+git://github.com/denisenkom/pytds.git#egg=pytds
fi
if [ $BACKEND = sqlserver.pymssql ]; then
    python env/bin/pip install cython hg+https://denisenkom@code.google.com/r/denisenkom-pymssql/ --use-mirrors
fi
python env/bin/pip install pytz==2013d --use-mirrors
export COMPUTERNAME=$HOST
python tests/runtests.py --noinput --settings=test_mssql
