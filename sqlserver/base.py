"""Microsoft SQL Server database backend for Django."""
from __future__ import absolute_import, unicode_literals

import warnings

from django.utils.timezone import utc

import sqlserver_ado
import sqlserver_ado.base

try:
    from sqlserver_ado import dbapi as ado_dbapi
    __import__('pythoncom')
except ImportError:
    ado_dbapi = None

try:
    import pytds
except ImportError:
    pytds = None

if pytds is not None:
    Database = pytds
elif ado_dbapi is not None:
    Database = ado_dbapi
else:
    raise Exception('Both ado and pytds are not available, to install pytds run pip install python-tds')

from sqlserver_ado.introspection import DatabaseIntrospection
from .operations import DatabaseOperations
from .creation import DatabaseCreation

try:
    import pytz
except ImportError:
    pytz = None

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError


_SUPPORTED_OPTIONS = ['failover_partner']


def utc_tzinfo_factory(offset):
    if offset != 0:
        raise AssertionError("database connection isn't set to UTC")
    return utc


class _CursorWrapper(object):
    """Used to intercept database errors for cursor's __next__ method"""
    def __init__(self, cursor, error_wrapper):
        self._cursor = cursor
        self._error_wrapper = error_wrapper
        self.execute = cursor.execute
        self.fetchall = cursor.fetchall

    def __getattr__(self, attr):
        return getattr(self._cursor, attr)

    def __iter__(self):
        with self._error_wrapper:
            for item in self._cursor:
                yield item


class DatabaseFeatures(sqlserver_ado.base.DatabaseFeatures):
    # Dict of test import path and list of versions on which it fails

    has_select_for_update = True
    has_select_for_update_nowait = True
    for_update_after_from = True


class DatabaseWrapper(sqlserver_ado.base.DatabaseWrapper):
    Database = Database

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        if self.Database is pytds:
            self.get_connection_params = self.get_connection_params_pytds
            self.create_cursor = self.create_cursor_pytds
            self.__get_dbms_version = self.__get_dbms_version_pytds
            self._set_autocommit = self._set_autocommit_pytds

    def get_connection_params_pytds(self):
        """Returns a dict of parameters suitable for get_new_connection."""
        from django.conf import settings
        settings_dict = self.settings_dict
        options = settings_dict.get('OPTIONS', {})
        autocommit = options.get('autocommit', False)
        conn_params = {
            'server': settings_dict['HOST'],
            'database': settings_dict['NAME'],
            'user': settings_dict['USER'],
            'port': settings_dict.get('PORT', '1433'),
            'password': settings_dict['PASSWORD'],
            'timeout': self.command_timeout,
            'autocommit': autocommit,
            'use_mars': options.get('use_mars', False),
            'load_balancer': options.get('load_balancer', None),
            'failover_partner': options.get('failover_partner', None),
            'use_tz': utc if getattr(settings, 'USE_TZ', False) else None,
         }

        for opt in _SUPPORTED_OPTIONS:
            if opt in options:
                conn_params[opt] = options[opt]

        self.tzinfo_factory = utc_tzinfo_factory if settings.USE_TZ else None

        return conn_params

    def get_new_connection(self, conn_params):
        """Opens a connection to the database."""
        self.__connection_string = conn_params.get('connection_string', '')
        conn = self.Database.connect(**conn_params)
        return conn

    def init_connection_state(self):
        """Initializes the database connection settings."""
        # if 'mars connection=true' in self.__connection_string.lower():
        #     # Issue #41 - Cannot use MARS with savepoints
        #     self.features.uses_savepoints = False
        # cache the properties on the connection
        if hasattr(self.connection, 'adoConn'):
            self.connection.adoConnProperties = dict([(x.Name, x.Value) for x in self.connection.adoConn.Properties])

        try:
            sql_version = int(self.__get_dbms_version().split('.', 2)[0])
        except (IndexError, ValueError):
            warnings.warn(
                "Unable to determine MS SQL server version. Only SQL 2008 or "
                "newer is supported.", DeprecationWarning)
        else:
            if sql_version < sqlserver_ado.base.VERSION_SQL2012:
                warnings.warn(
                    "This version of MS SQL server is no longer tested with "
                    "django-mssql and not officially supported/maintained.",
                    DeprecationWarning)
        if self.Database is pytds:
            self.features.supports_paramstyle_pyformat = True
            # only pytds support new sql server date types
            self.features.supports_microsecond_precision = True
        if self.settings_dict["OPTIONS"].get("allow_nulls_in_unique_constraints", True):
            self.features.ignores_nulls_in_unique_constraints = True
            self.features.supports_nullable_unique_constraints = True
            self.features.supports_partially_nullable_unique_constraints = True

    def create_cursor_pytds(self):
        """Creates a cursor. Assumes that a connection is established."""
        cursor = self.connection.cursor()
        cursor.tzinfo_factory = self.tzinfo_factory
        error_wrapper = self.wrap_database_errors
        return _CursorWrapper(cursor, error_wrapper)

    def _set_autocommit_pytds(self, value):
        self.connection.autocommit = value

    def __get_dbms_version_pytds(self, make_connection=True):
        """
        Returns the 'DBMS Version' string
        """
        if not self.connection and make_connection:
            self.connect()
        major = (self.connection.product_version & 0xff000000) >> 24
        minor = (self.connection.product_version & 0xff0000) >> 16
        return '{}.{}'.format(major, minor)

    def is_usable(self):
        try:
            # Use a mssql cursor directly, bypassing Django's utilities.
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        except:
            return False
        else:
            return True
