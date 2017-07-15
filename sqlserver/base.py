"""Microsoft SQL Server database backend for Django."""
from __future__ import absolute_import, unicode_literals

import warnings

import django
from django.db.backends.base.client import BaseDatabaseClient
from django.utils.timezone import utc

import sqlserver_ado
import sqlserver_ado.base

try:
    import pytds
except ImportError:
    raise Exception('pytds is not available, to install pytds run pip install python-tds')

from sqlserver_ado.introspection import DatabaseIntrospection
from .operations import DatabaseOperations
from sqlserver_ado.creation import DatabaseCreation

try:
    import pytz
except ImportError:
    pytz = None

DatabaseError = pytds.DatabaseError
IntegrityError = pytds.IntegrityError


_SUPPORTED_OPTIONS = ['failover_partner']


def utc_tzinfo_factory(offset):
    if offset != 0:
        raise AssertionError("database connection isn't set to UTC")
    return utc


class DatabaseFeatures(sqlserver_ado.base.DatabaseFeatures):
    # Dict of test import path and list of versions on which it fails

    if django.VERSION >= (1, 11, 0):
        has_select_for_update = True
        has_select_for_update_nowait = True
        has_select_for_update_skip_locked = True
        for_update_after_from = True

    # mssql does not have bit shift operations
    # but we can implement such using */ 2^x
    supports_bitwise_leftshift = False
    supports_bitwise_rightshift = False

    # probably can be implemented
    can_introspect_default = False


class DatabaseWrapper(sqlserver_ado.base.DatabaseWrapper):
    Database = pytds
    # Classes instantiated in __init__().
    client_class = BaseDatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        # following lines can be removed once django-mssql
        # is doing the same as below
        self.features = self.features_class(self)
        self.ops = self.ops_class(self)
        self.introspection = self.introspection_class(self)

    def get_connection_params(self):
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
        self.features.supports_paramstyle_pyformat = True
        if self.settings_dict["OPTIONS"].get("allow_nulls_in_unique_constraints", True):
            self.features.ignores_nulls_in_unique_constraints = True
            self.features.supports_nullable_unique_constraints = True
            self.features.supports_partially_nullable_unique_constraints = True

    def create_cursor(self, name=None):
        """Creates a cursor. Assumes that a connection is established."""
        cursor = self.connection.cursor()
        cursor.tzinfo_factory = self.tzinfo_factory
        return cursor

    def __get_dbms_version(self, make_connection=True):
        """
        Returns the 'DBMS Version' string
        """
        major, minor, _, _ = self.get_server_version(make_connection=make_connection)
        return '{}.{}'.format(major, minor)

    def get_server_version(self, make_connection=True):
        if not self.connection and make_connection:
            self.connect()
        major = (self.connection.product_version & 0xff000000) >> 24
        minor = (self.connection.product_version & 0xff0000) >> 16
        p1 = (self.connection.product_version & 0xff00) >> 8
        p2 = self.connection.product_version & 0xff
        return major, minor, p1, p2
