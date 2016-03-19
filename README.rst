Django MSSQL Database Backend
=============================

A minimal wrapper for django-mssql to make it work with python-tds.

This package will try to connect using ADO first, and if it is not
available it will try python-tds second.

In addition to django-mssql features this package also supports:

- select_for_update
- multiple NULLs in unique constraints

Installation
------------

.. code-block::

   pip install git+https://bitbucket.org/Manfre/django-mssql.git#egg=django-mssql
   pip install git+https://github.com/denisenkom/django-sqlserver.git#egg=django-sqlserver


Requirements
------------

- django-mssql, version 1.6.x
- Python 2.7 or 3.3

SQL Server versions
-------------------

- 2012

Status
------

Works on python-tds, ADO not tested yet, but should work too.
Django 1.7 migration tests are not fully passing yet.

License
-------

MIT

Known Issues
------------

- Doesn't work with old DATETIME columns.  To use this package you should change all DATETIME columns
  to DATETIME2(6).

Testing
-------

This project integrates with Django's own test suite.  To run tests you need to install Microsoft SQL Server
2012 or newer.  In SQL server create user sa with password sa, alternatively you can use different user but in this
case you should set SQLUSER and SQLPASSWORD environment variables.

If you use SQL server instance name provide it in SQLINSTANCE environment variable.

If your SQL server runs on host different from localhost you need to provide name of the host in HOST environment
variable.

By default tests are run against Django 1.9, if you want to test against different version specify it in DJANGO_VER
environment variable.

By default tests are run using default Python, if you want to use different Python version specify it's location
in PYTHONHOME environment variable.

Now on Windows you can run:

  ci.bat

If you are on UNIX run:

  ci.sh
