set ROOT=%CD%
if not defined DJANGO_VER set DJANGO_VER=1.9
if defined PYTHONHOME (set virtualenv=%PYTHONHOME%\scripts\virtualenv) else (set virtualenv=virtualenv)

:: using system-site-packages to get pywin32 package which is not installable via pip
%virtualenv% env --system-site-packages
if %errorlevel% neq 0 exit /b %errorlevel%

set PYTHONHOME=

set django_branch=stable/%DJANGO_VER%.x

:: cloning Django repository
if not exist env\src\django call git clone https://github.com/django/django.git env/src/django

pushd env\src\django

call git pull
if %errorlevel% neq 0 exit /b %errorlevel%

call git checkout %django_branch%
if %errorlevel% neq 0 exit /b %errorlevel%

popd

env\scripts\pip install -e env\src\django
if %errorlevel% neq 0 exit /b %errorlevel%

:: cloning pytds repository
if not exist pytds call git clone https://github.com/denisenkom/pytds.git

pushd pytds

call git pull
if %errorlevel% neq 0 exit /b %errorlevel%

popd

env\scripts\pip install -e .\pytds
if %errorlevel% neq 0 exit /b %errorlevel%

:: cloning django-mssql repository
if not exist django-mssql call git clone git@bitbucket.org:denisenkom/django-mssql.git -b django1.9-support

pushd django-mssql

call git pull
if %errorlevel% neq 0 exit /b %errorlevel%

popd

env\scripts\pip install -e .\django-mssql
if %errorlevel% neq 0 exit /b %errorlevel%


env\scripts\pip install pytz==2013d
if %errorlevel% neq 0 exit /b %errorlevel%

set COMPUTERNAME=%HOST%

env\scripts\pip install -e .

set PYTHONPATH=%ROOT%\tests

echo Running Django test suite...
env\scripts\python env\src\django\tests\runtests.py --noinput --settings=test_mssql
