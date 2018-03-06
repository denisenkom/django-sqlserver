"""Microsoft SQL Server database backend for Django."""
from __future__ import absolute_import, unicode_literals
import datetime
import collections

import django.db.backends.base.client
from django.utils.timezone import utc

import sqlserver_ado
import sqlserver_ado.base
import sqlserver_ado.compiler
import sqlserver_ado.operations
import sqlserver_ado.introspection
import sqlserver_ado.creation

try:
    import pytds
except ImportError:
    raise Exception('pytds is not available, to install pytds run pip install python-tds')

try:
    import pytz
except ImportError:
    pytz = None

DatabaseError = pytds.DatabaseError
IntegrityError = pytds.IntegrityError


_SUPPORTED_OPTIONS = [
    'dsn', 'timeout',
    'login_timeout', 'as_dict',
    'appname', 'tds_version',
    'blocksize', 'auth',
    'readonly', 'bytes_to_unicode',
    'row_strategy', 'cafile',
    'validate_host', 'enc_login_only',
]


def utc_tzinfo_factory(offset):
    if offset != 0:
        raise AssertionError("database connection isn't set to UTC")
    return utc


#
# Main class which uses pytds as a driver instead of adodb
#
class DatabaseWrapper(sqlserver_ado.base.DatabaseWrapper):
    Database = pytds

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


#
# Next goes monkey patches which can be removed once those changes are merged into respective packages
#

#
# monkey patch DatabaseFeatures class
#
if django.VERSION >= (1, 11, 0):
    sqlserver_ado.base.DatabaseFeatures.has_select_for_update = True
    sqlserver_ado.base.DatabaseFeatures.has_select_for_update_nowait = True
    sqlserver_ado.base.DatabaseFeatures.has_select_for_update_skip_locked = True
    sqlserver_ado.base.DatabaseFeatures.for_update_after_from = True

# mssql does not have bit shift operations
# but we can implement such using */ 2^x
sqlserver_ado.base.DatabaseFeatures.supports_bitwise_leftshift = False
sqlserver_ado.base.DatabaseFeatures.supports_bitwise_rightshift = False

# probably can be implemented
sqlserver_ado.base.DatabaseFeatures.can_introspect_default = False


#
# monkey patch SQLCompiler class
#
def _call_base_as_sql_old(self, with_limits=True, with_col_aliases=False, subquery=False):
    return super(sqlserver_ado.compiler.SQLCompiler, self).as_sql(
        with_limits=with_limits,
        with_col_aliases=with_col_aliases,
        subquery=subquery,
    )


def _call_base_as_sql_new(self, with_limits=True, with_col_aliases=False, subquery=False):
    return super(sqlserver_ado.compiler.SQLCompiler, self).as_sql(
        with_limits=with_limits,
        with_col_aliases=with_col_aliases,
    )


def _as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
    # Get out of the way if we're not a select query or there's no limiting involved.
    has_limit_offset = with_limits and (self.query.low_mark or self.query.high_mark is not None)
    try:
        if not has_limit_offset:
            # The ORDER BY clause is invalid in views, inline functions,
            # derived tables, subqueries, and common table expressions,
            # unless TOP or FOR XML is also specified.
            setattr(self.query, '_mssql_ordering_not_allowed', with_col_aliases)

        # let the base do its thing, but we'll handle limit/offset
        sql, fields = self._call_base_as_sql(
            with_limits=False,
            with_col_aliases=with_col_aliases,
            subquery=subquery,
        )

        if has_limit_offset:
            if ' order by ' not in sql.lower():
                # Must have an ORDER BY to slice using OFFSET/FETCH. If
                # there is none, use the first column, which is typically a
                # PK
                sql += ' ORDER BY 1'
            sql += ' OFFSET %d ROWS' % (self.query.low_mark or 0)
            if self.query.high_mark is not None:
                sql += ' FETCH NEXT %d ROWS ONLY' % (self.query.high_mark - self.query.low_mark)
    finally:
        if not has_limit_offset:
            # remove in case query is ever reused
            delattr(self.query, '_mssql_ordering_not_allowed')

    return sql, fields


if django.VERSION < (1, 11, 0):
    sqlserver_ado.compiler.SQLCompiler._call_base_as_sql = _call_base_as_sql_old
else:
    sqlserver_ado.compiler.SQLCompiler._call_base_as_sql = _call_base_as_sql_new
sqlserver_ado.compiler.SQLCompiler.as_sql = _as_sql

#
# monkey patch DatabaseOperations to support select_for_update
#
def _for_update_sql(self, nowait=False, skip_locked=False):
    hints = ['ROWLOCK', 'UPDLOCK']
    if nowait:
        hints += ['NOWAIT']
    if skip_locked:
        hints += ['READPAST']
    return "WITH ({})".format(','.join(hints))


def _value_to_db_date(self, value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        value = value.date()
    return value.isoformat()


sqlserver_ado.operations.DatabaseOperations.for_update_sql = _for_update_sql
sqlserver_ado.operations.DatabaseOperations.value_to_db_date = _value_to_db_date


# monkey patch adoConn property onto connection class which is expected by django-mssql
# this can be removed if django-mssql would not use this property
pytds.Connection.adoConn = collections.namedtuple('AdoConn', 'Properties')(Properties=[])


#
# monkey patch sqlserver_ado.base.DatabaseWrapper class
#
def _get_new_connection(self, conn_params):
    """Opens a connection to the database."""
    self.__connection_string = conn_params.get('connection_string', '')
    conn = self.Database.connect(**conn_params)
    return conn


sqlserver_ado.base.DatabaseWrapper.get_new_connection = _get_new_connection
sqlserver_ado.base.DatabaseWrapper.client_class = django.db.backends.base.client.BaseDatabaseClient
sqlserver_ado.base.DatabaseWrapper.creation_class = sqlserver_ado.creation.DatabaseCreation
sqlserver_ado.base.DatabaseWrapper.features_class = sqlserver_ado.base.DatabaseFeatures
sqlserver_ado.base.DatabaseWrapper.introspection_class = sqlserver_ado.introspection.DatabaseIntrospection
sqlserver_ado.base.DatabaseWrapper.ops_class = sqlserver_ado.operations.DatabaseOperations

