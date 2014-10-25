Django MSSQL Database Backend
=============================

A minimal wrapper for django-mssql to make it work with python-tds.

This package will try to connect using ADO first, and if it is not
available it will try python-tds second.

In addition to django-mssql features this package also supports:

- select_for_update
- multiple NULLs in unique constraints
- MSSQL 2008 support (2008 support is currently dropped in django-mssql)

Status
------

Works on python-tds, ADO not tested yet, but should work too.
Django 1.7 migration tests are not fully passing yet.

License
-------

MIT
