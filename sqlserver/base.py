"""Microsoft SQL Server database backend for Django."""
from __future__ import absolute_import, unicode_literals

import warnings

from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.db.backends import BaseDatabaseWrapper, BaseDatabaseFeatures, BaseDatabaseValidation, BaseDatabaseClient
from django.db.utils import IntegrityError as DjangoIntegrityError, \
    InterfaceError as DjangoInterfaceError
from django.utils.functional import cached_property
from django.utils import six
from django.utils.timezone import utc

try:
    from . import dbapi as ado_dbapi
    import pythoncom
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

from .introspection import DatabaseIntrospection
from .creation import DatabaseCreation
from .operations import DatabaseOperations
try:
    from .schema import DatabaseSchemaEditor
except ImportError:
    DatabaseSchemaEditor = None

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


class DatabaseFeatures(BaseDatabaseFeatures):
    uses_custom_query_class = True
    has_bulk_insert = True

    # DateTimeField doesn't support timezones, only DateTimeOffsetField
    supports_timezones = False
    supports_sequence_reset = False

    can_return_id_from_insert = True

    supports_regex_backreferencing = False

    supports_tablespaces = True

    # Django < 1.7
    ignores_nulls_in_unique_constraints = False
    # Django >= 1.7
    supports_nullable_unique_constraints = False
    supports_partially_nullable_unique_constraints = False

    can_introspect_autofield = True
    can_introspect_small_integer_field = True

    supports_subqueries_in_group_by = False

    allow_sliced_subqueries = False

    uses_savepoints = True

    supports_paramstyle_pyformat = False

    closed_cursor_error_class = DjangoInterfaceError

    # connection_persists_old_columns = True

    requires_literal_defaults = True

    @cached_property
    def has_zoneinfo_database(self):
        return pytz is not None

    # Dict of test import path and list of versions on which it fails
    failing_tests = {
        # Some tests are known to fail with django-mssql.
        'aggregation.tests.BaseAggregateTestCase.test_dates_with_aggregation': [(1, 6), (1, 7)],
        'aggregation_regress.tests.AggregationTests.test_more_more_more': [(1, 6), (1, 7)],

        # this test is invalid in Django 1.6
        # it expects db driver to return incorrect value for id field, when
        # mssql returns correct value
        'introspection.tests.IntrospectionTests.test_get_table_description_types': [(1, 6)],

        # this test is invalid in Django 1.6
        # it expects db driver to return incorrect value for id field, when
        # mssql returns correct value
        'inspectdb.tests.InspectDBTestCase.test_number_field_types': [(1, 6)],

        # MSSQL throws an arithmetic overflow error.
        'expressions_regress.tests.ExpressionOperatorTests.test_righthand_power': [(1, 7)],

        # The migrations and schema tests also fail massively at this time.
        'migrations.test_operations.OperationTests.test_alter_field_pk': [(1, 7)],

        # Those tests use case-insensitive comparison which is not supported correctly by MSSQL
        'get_object_or_404.tests.GetObjectOr404Tests.test_get_object_or_404': [(1, 6), (1, 7)],
        'queries.tests.ComparisonTests.test_ticket8597': [(1, 6), (1, 7)],

        # This test fails on MSSQL because it can't make DST corrections
        'datetimes.tests.DateTimesTests.test_21432': [(1, 6), (1, 7)],
    }

    has_select_for_update = True
    has_select_for_update_nowait = False


def is_ip_address(value):
    """
    Returns True if value is a valid IP address, otherwise False.
    """
    # IPv6 added with Django 1.4
    from django.core.validators import validate_ipv46_address as ip_validator

    try:
        ip_validator(value)
    except ValidationError:
        return False
    return True


def connection_string_from_settings():
    from django.conf import settings
    db_settings = getattr(settings, 'DATABASES', {}).get('default', None)
    return make_connection_string(db_settings)


def make_connection_string(settings):
    db_name = settings['NAME'].strip()
    db_host = settings['HOST'] or '127.0.0.1'
    db_port = settings['PORT']
    db_user = settings['USER']
    db_password = settings['PASSWORD']
    options = settings.get('OPTIONS', {})

    if len(db_name) == 0:
        raise ImproperlyConfigured("You need to specify a DATABASE NAME in your Django settings file.")

    # Connection strings courtesy of:
    # http://www.connectionstrings.com/?carrier=sqlserver

    # If a port is given, force a TCP/IP connection. The host should be an IP address in this case.
    if db_port:
        if not is_ip_address(db_host):
            raise ImproperlyConfigured("When using DATABASE PORT, DATABASE HOST must be an IP address.")
        try:
            db_port = int(db_port)
        except ValueError:
            raise ImproperlyConfigured("DATABASE PORT must be a number.")
        db_host = '{0},{1};Network Library=DBMSSOCN'.format(db_host, db_port)

    # If no user is specified, use integrated security.
    if db_user != '':
        auth_string = 'UID={0};PWD={1}'.format(db_user, db_password)
    else:
        auth_string = 'Integrated Security=SSPI'

    parts = [
        'DATA SOURCE={0};Initial Catalog={1}'.format(db_host, db_name),
        auth_string
    ]

    if not options.get('provider', None):
        options['provider'] = 'sqlncli10'

    parts.append('PROVIDER={0}'.format(options['provider']))

    extra_params = options.get('extra_params', '')

    if 'sqlncli' in options['provider'].lower() and 'datatypecompatibility=' not in extra_params.lower():
        # native client needs a compatibility mode that behaves like OLEDB
        parts.append('DataTypeCompatibility=80')

    if options.get('use_mars', True) and 'mars connection=' not in extra_params.lower():
        parts.append('MARS Connection=True')

    if extra_params:
        parts.append(options['extra_params'])

    return ";".join(parts)


VERSION_SQL2000 = 8
VERSION_SQL2005 = 9
VERSION_SQL2008 = 10
VERSION_SQL2012 = 11


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'microsoft'

    Database = Database

    operators = {
        "exact": "= %s",
        "iexact": "LIKE %s ESCAPE '\\'",
        "contains": "LIKE %s ESCAPE '\\'",
        "icontains": "LIKE %s ESCAPE '\\'",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE %s ESCAPE '\\'",
        "endswith": "LIKE %s ESCAPE '\\'",
        "istartswith": "LIKE %s ESCAPE '\\'",
        "iendswith": "LIKE %s ESCAPE '\\'",
    }

    def __init__(self, *args, **kwargs):
        self.use_transactions = kwargs.pop('use_transactions', None)

        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        try:
            self.command_timeout = int(self.settings_dict.get('COMMAND_TIMEOUT', 30))
        except ValueError:
            self.command_timeout = 30

        options = self.settings_dict.get('OPTIONS', {})
        try:
            self.cast_avg_to_float = not bool(options.get('disable_avg_cast', False))
        except ValueError:
            self.cast_avg_to_float = False

        USE_LEGACY_DATE_FIELDS_DEFAULT = False
        try:
            self.use_legacy_date_fields = bool(options.get('use_legacy_date_fields', USE_LEGACY_DATE_FIELDS_DEFAULT))
        except ValueError:
            self.use_legacy_date_fields = USE_LEGACY_DATE_FIELDS_DEFAULT

        if self.use_legacy_date_fields:
                warnings.warn(
                    "The `use_legacy_date_fields` setting has been deprecated. "
                    "The default option value has changed to 'False'. "
                    "If you need to use the legacy SQL 'datetime' datatype, "
                    "you must replace them with the provide model field.",
                    DeprecationWarning)

        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = BaseDatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation(self)
        if self.Database is pytds:
            self.get_connection_params = self.get_connection_params_pytds
            self.create_cursor = self.create_cursor_pytds
            self.__get_dbms_version = self.__get_dbms_version_pytds
            self._set_autocommit = self._set_autocommit_pytds

    def get_connection_params(self):
        """Returns a dict of parameters suitable for get_new_connection."""
        settings_dict = self.settings_dict.copy()
        if settings_dict['NAME'] == '':
            from django.core.exceptions import ImproperlyConfigured
            raise ImproperlyConfigured(
                "settings.DATABASES is improperly configured. "
                "Please supply the NAME value.")
        if not settings_dict['NAME']:
            # if _nodb_connection, connect to master
            settings_dict['NAME'] = 'master'

        autocommit = settings_dict.get('OPTIONS', {}).get('autocommit', False)
        return {
            'connection_string': make_connection_string(settings_dict),
            'timeout': self.command_timeout,
            'use_transactions': not autocommit,
        }

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
        conn = Database.connect(**conn_params)
        self.creation.sql_create_model = self.creation.sql_create_model_sql2008
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
            if sql_version < VERSION_SQL2008:
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

    def create_cursor_pytds(self):
        """Creates a cursor. Assumes that a connection is established."""
        cursor = self.connection.cursor()
        cursor.tzinfo_factory = self.tzinfo_factory
        error_wrapper = self.wrap_database_errors
        return _CursorWrapper(cursor, error_wrapper)

    def create_cursor(self):
        """Creates a cursor. Assumes that a connection is established."""
        cursor = self.connection.cursor()
        return cursor

    def _set_autocommit(self, value):
        self.connection.set_autocommit(value)

    def _set_autocommit_pytds(self, value):
        self.connection.autocommit = value

    def __get_dbms_version(self, make_connection=True):
        """
        Returns the 'DBMS Version' string, or ''. If a connection to the database has not already
        been established, a connection will be made when `make_connection` is True.
        """
        if not self.connection and make_connection:
            self.connect()
        return self.connection.adoConnProperties.get('DBMS Version', '') if self.connection else ''

    def __get_dbms_version_pytds(self, make_connection=True):
        """
        Returns the 'DBMS Version' string
        """
        if not self.connection and make_connection:
            self.connect()
        major = (self.connection.product_version & 0xff000000) >> 24
        minor = (self.connection.product_version & 0xff0000) >> 16
        return '{}.{}'.format(major, minor)

    def _is_sql2005_and_up(self, conn):
        return int(self.__get_dbms_version(conn).split('.')[0]) >= VERSION_SQL2005

    def _is_sql2008_and_up(self, conn):
        return int(self.__get_dbms_version(conn).split('.')[0]) >= VERSION_SQL2008

    def disable_constraint_checking(self):
        """
        Turn off constraint checking for every table
        """
        if self.connection:
            cursor = self.connection.cursor()
        else:
            cursor = self._cursor()
        cursor.execute('EXEC sp_MSforeachtable "ALTER TABLE ? NOCHECK CONSTRAINT all"')
        return True

    def enable_constraint_checking(self):
        """
        Turn on constraint checking for every table
        """
        if self.connection:
            cursor = self.connection.cursor()
        else:
            cursor = self._cursor()
        # don't check the data, just turn them on
        cursor.execute('EXEC sp_MSforeachtable "ALTER TABLE ? WITH NOCHECK CHECK CONSTRAINT all"')

    def check_constraints(self, table_names=None):
        """
        Check the table constraints.
        """
        if self.connection:
            cursor = self.connection.cursor()
        else:
            cursor = self._cursor()
        if not table_names:
            cursor.execute('DBCC CHECKCONSTRAINTS WITH ALL_CONSTRAINTS')
            if cursor.description:
                raise DjangoIntegrityError(cursor.fetchall())
        else:
            qn = self.ops.quote_name
            for name in table_names:
                cursor.execute('DBCC CHECKCONSTRAINTS({0}) WITH ALL_CONSTRAINTS'.format(
                    qn(name)
                ))
                if cursor.description:
                    raise DjangoIntegrityError(cursor.fetchall())

    # # MS SQL Server doesn't support explicit savepoint commits; savepoints are
    # # implicitly committed with the transaction.
    # # Ignore them.
    def _savepoint_commit(self, sid):
        try:
            queries_log = self.queries_log   # Django 1.8+
        except AttributeError:
            queries_log = self.queries       # Django <1.8
        if queries_log:
            queries_log.append({
                'sql': '-- RELEASE SAVEPOINT %s -- (because assertNumQueries)' % self.ops.quote_name(sid),
                'time': '0.000',
            })

    def is_usable(self):
        try:
            # Use a mssql cursor directly, bypassing Django's utilities.
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        except:
            return False
        else:
            return True

    def schema_editor(self, *args, **kwargs):
        """Returns a new instance of this backend's SchemaEditor"""
        return DatabaseSchemaEditor(self, *args, **kwargs)
