Django MSSQL Database Backend
=============================

.. image:: https://ci.appveyor.com/api/projects/status/hj5o8fhpllcfypte/branch/master?svg=true
    :target: https://ci.appveyor.com/project/denisenkom/django-sqlserver

.. image:: https://codecov.io/gh/denisenkom/django-sqlserver/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/denisenkom/django-sqlserver

A minimal wrapper for django-mssql to make it work with python-tds.

In addition to django-mssql features this package also supports:

- select_for_update
- multiple NULLs in unique constraints

Supported MSSQL versions:

- 2012
- 2014
- 2016

Requirements
------------

- django-mssql, version 1.6.x
- Python 2.7 or 3.6

Installation
------------

.. code-block::

   pip install git+https://bitbucket.org/Manfre/django-mssql.git#egg=django-mssql
   pip install git+https://github.com/denisenkom/django-sqlserver.git#egg=django-sqlserver

Configuration
-------------

You would need to add database configuration, here is example:

.. code-block:: python

    DATABASES = {
        'default': {
            'ENGINE': 'sqlserver',
            'HOST': 'mysqlserverhost\\instance',  # Replace with host name where you have MSSQL server running
            'NAME': 'mydbname',  # Replace with name of the database on the MSSQL server
            'USER': 'username',  # Replace with user name
            'PASSWORD': '*****',  # Replace with password
        },


You can also specify additional OPTIONS attribute as described in
http://django-mssql.readthedocs.io/en/latest/settings.html#options

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

This project integrates with Django's own test suite.

You need to install Python 2.7 or newer.

You need to install virtualenv.

To run tests you need to install Microsoft SQL Server 2012 or newer.

In SQL server create user sa with password sa, alternatively you can use different user but in this
case you should set SQLUSER and SQLPASSWORD environment variables.

You should enable TCP/IP connections for SQL server.

If you use SQL server instance name provide it in SQLINSTANCE environment variable.

If your SQL server runs on host different from localhost you need to provide name of the host in HOST environment
variable.

To run test run:

  python acceptance_test.py
