from __future__ import absolute_import, unicode_literals

from contextlib import contextmanager
import sys
import time

import django
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django.db.backends.creation import BaseDatabaseCreation
from django.utils import six
from django.utils.functional import cached_property

from unittest import expectedFailure

try:
    from django.utils.module_loading import import_string   # Django >= 1.7
except ImportError:
    from django.utils.module_loading import import_by_path as import_string


IS_DJANGO_16 = django.VERSION[0] == 1 and django.VERSION[1] == 6

try:
    from django.db.backends.creation import NO_DB_ALIAS
except ImportError:
    NO_DB_ALIAS = '__no_db__'


class DatabaseCreation(BaseDatabaseCreation):
    # This dictionary maps Field objects to their associated Server Server column
    # types, as strings. Column-type strings can contain format strings; they'll
    # be interpolated against the values of Field.__dict__.
    data_types = {
        'AutoField':                    'int IDENTITY (1, 1)',
        'BigAutoField':                 'bigint IDENTITY (1, 1)',
        'BigIntegerField':              'bigint',
        'BinaryField':                  'varbinary(max)',
        'BooleanField':                 'bit',
        'CharField':                    'nvarchar(%(max_length)s)',
        'CommaSeparatedIntegerField':   'nvarchar(%(max_length)s)',
        'DateField':                    'date',
        'DateTimeField':                'datetime2',
        'DateTimeOffsetField':          'datetimeoffset',
        'DecimalField':                 'decimal(%(max_digits)s, %(decimal_places)s)',
        'FileField':                    'nvarchar(%(max_length)s)',
        'FilePathField':                'nvarchar(%(max_length)s)',
        'FloatField':                   'double precision',
        'GenericIPAddressField':        'nvarchar(39)',
        'IntegerField':                 'int',
        'IPAddressField':               'nvarchar(15)',
        'LegacyDateField':              'datetime',
        'LegacyDateTimeField':          'datetime',
        'LegacyTimeField':              'time',
        'NewDateField':                 'date',
        'NewDateTimeField':             'datetime2',
        'NewTimeField':                 'time',
        'NullBooleanField':             'bit',
        'OneToOneField':                'int',
        'PositiveIntegerField':         'int',
        'PositiveSmallIntegerField':    'smallint',
        'SlugField':                    'nvarchar(%(max_length)s)',
        'SmallIntegerField':            'smallint',
        'TextField':                    'nvarchar(max)',
        'TimeField':                    'time',
    }

    # Starting with Django 1.7, check constraints are no longer included in with
    # the data_types value.
    data_type_check_constraints = {
        'PositiveIntegerField': '%(qn_column)s >= 0',
        'PositiveSmallIntegerField': '%(qn_column)s >= 0',
    }

    def __init__(self, *args, **kwargs):
        if IS_DJANGO_16:
            # Django 1.6 expects the data type to contain the CHECK constraint
            self.data_types['PositiveIntegerField'] = 'int CHECK ([%(column)s] >= 0)'
            self.data_types['PositiveSmallIntegerField'] = 'smallint CHECK ([%(column)s] >= 0)'

        super(DatabaseCreation, self).__init__(*args, **kwargs)

        if self.connection.use_legacy_date_fields:
            self.data_types.update({
                'DateField': 'datetime',
                'DateTimeField': 'datetime',
                'TimeField': 'datetime',
            })


    def _create_master_connection(self):
        """
        Create a transactionless connection to 'master' database.
        """
        settings_dict = self.connection.settings_dict.copy()
        settings_dict['NAME'] = 'master'
        nodb_connection = type(self.connection)(
            settings_dict,
            alias=NO_DB_ALIAS,
            allow_thread_sharing=False)
        return nodb_connection
    # Override on 1.7 and add to 1.6
    _nodb_connection = cached_property(_create_master_connection)

    def mark_tests_as_expected_failure(self, failing_tests):
        """
        Flag tests as expectedFailure. This should only run during the
        testsuite.
        """
        django_version = django.VERSION[:2]
        for test_name, versions in six.iteritems(failing_tests):
            if not versions or not isinstance(versions, (list, tuple)):
                # skip None, empty, or invalid
                continue
            if not isinstance(versions[0], (list, tuple)):
                # Ensure list of versions
                versions = [versions]
            if all(map(lambda v: v[:2] != django_version, versions)):
                continue
            test_case_name, _, method_name = test_name.rpartition('.')
            try:
                test_case = import_string(test_case_name)
            except (ImportError, ImproperlyConfigured):
                continue
            method = getattr(test_case, method_name)
            method = expectedFailure(method)
            setattr(test_case, method_name, method)

    def create_test_db(self, *args, **kwargs):
        self.mark_tests_as_expected_failure(self.connection.features.failing_tests)
        super(DatabaseCreation, self).create_test_db(*args, **kwargs)

    def _create_test_db(self, verbosity=1, autoclobber=False):
        """
        Create the test databases using a connection to database 'master'.
        """
        if self._test_database_create(settings):
            try:
                with use_master_connection(self):
                    test_database_name = super(DatabaseCreation, self)._create_test_db(verbosity, autoclobber)
            except Exception as e:
                if 'Choose a different database name.' in str(e):
                    six.print_('Database "%s" could not be created because it already exists.' % test_database_name)
                else:
                    six.reraise(*sys.exc_info())
            self.install_regex_clr(test_database_name)
            return test_database_name

        if verbosity >= 1:
            six.print_("Skipping Test DB creation")
        return self._get_test_db_name()

    def _destroy_test_db(self, test_database_name, verbosity=1):
        """
        Drop the test databases using a connection to database 'master'.
        """
        if not self._test_database_create(settings):
            if verbosity >= 1:
                six.print_("Skipping Test DB destruction")
            return

        for alias in connections:
            connections[alias].close()
        try:
            with self._nodb_connection.cursor() as cursor:
                qn_db_name = self.connection.ops.quote_name(test_database_name)
                # boot all other connections to the database, leaving only this connection
                cursor.execute("ALTER DATABASE %s SET SINGLE_USER WITH ROLLBACK IMMEDIATE" % qn_db_name)
                time.sleep(1)
                # database is now clear to drop
                cursor.execute("DROP DATABASE %s" % qn_db_name)
        except Exception:
            # if 'it is currently in use' in str(e):
            #     six.print_('Cannot drop database %s because it is in use' % test_database_name)
            # else:
                six.reraise(*sys.exc_info())

    def _test_database_create(self, settings):
        """
        Check the settings to see if the test database should be created.
        """
        if 'TEST_CREATE' in self.connection.settings_dict:
            return self.connection.settings_dict.get('TEST_CREATE', True)
        if hasattr(settings, 'TEST_DATABASE_CREATE'):
            return settings.TEST_DATABASE_CREATE
        else:
            return True

    def sql_create_model_sql2008(self, model, style, known_models=set()):
        opts = model._meta
        if not opts.managed or opts.proxy or getattr(opts, 'swapped', False):
            return [], {}
        qn = self.connection.ops.quote_name
        fix_fields = []
        queries = []
        for f in opts.local_fields:
            if not f.primary_key and f.unique and f.null:
                sql = style.SQL_KEYWORD('CREATE UNIQUE INDEX') +\
                    ' [idx_{table}_{column}] ' + style.SQL_KEYWORD('ON') +\
                    ' [{table}]([{column}]) ' + style.SQL_KEYWORD('WHERE') +\
                    ' [{column}] ' + style.SQL_KEYWORD('IS NOT NULL')
                queries.append(sql.format(table=opts.db_table,
                                          column=f.column))
                fix_fields.append(f)
                f._unique = False
        for field_constraints in opts.unique_together:
            predicate_parts = ['[{0}] '.format(opts.get_field(f).column) + style.SQL_KEYWORD('IS NOT NULL')
                               for f in field_constraints if opts.get_field(f).null]
            index_name = 'UX_{table}_{columns}'.format(
                table=opts.db_table,
                columns='_'.join(opts.get_field(f).column for f in field_constraints),
                )[:128]
            sql = style.SQL_KEYWORD('CREATE UNIQUE INDEX') +\
                ' {index_name} ' + style.SQL_KEYWORD('ON') +\
                ' {table}({columns_csep})'
            sql = sql.format(
                index_name=qn(index_name),
                table=style.SQL_TABLE(qn(opts.db_table)),
                columns_csep=','.join(qn(opts.get_field(f).column)
                                      for f in field_constraints),
                )
            if predicate_parts:
                sql += style.SQL_KEYWORD(' WHERE') + \
                    style.SQL_KEYWORD(' AND ').join(predicate_parts)
            queries.append(sql)
        unique_together = opts.unique_together
        opts.unique_together = []
        list_of_sql, pending_references_dict = super(DatabaseCreation, self).sql_create_model(model, style, known_models)
        opts.unique_together = unique_together
        for f in fix_fields:
            f._unique = True
        list_of_sql.extend(queries)
        return list_of_sql, pending_references_dict

    def install_regex_clr(self, database_name):
        sql = '''
USE {database_name};

-- This operation requires special permissions on the server
-- and is server-wide, shouldn't perform such operation.
-- TODO: make this optional, and only do it when clr is not enabled
-- Enable CLR in this database
--sp_configure 'show advanced options', 1;
--RECONFIGURE;
--sp_configure 'clr enabled', 1;
--RECONFIGURE;

-- Drop and recreate the function if it already exists
IF OBJECT_ID('REGEXP_LIKE') IS NOT NULL
    DROP FUNCTION [dbo].[REGEXP_LIKE]

IF EXISTS(select * from sys.assemblies where name like 'regex_clr')
    DROP ASSEMBLY regex_clr
;

CREATE ASSEMBLY regex_clr
FROM 0x{assembly_hex}
WITH PERMISSION_SET = SAFE;

create function [dbo].[REGEXP_LIKE]
(
    @input nvarchar(max),
    @pattern nvarchar(max),
    @caseSensitive int
)
RETURNS INT  AS
EXTERNAL NAME regex_clr.UserDefinedFunctions.REGEXP_LIKE
        '''.format(
            database_name=self.connection.ops.quote_name(database_name),
            assembly_hex=self.get_regex_clr_assembly_hex(),
        ).split(';')

        with self._nodb_connection.cursor() as cursor:
            for s in sql:
                cursor.execute(s)

    def get_regex_clr_assembly_hex(self):
        import os
        import binascii
        with open(os.path.join(os.path.dirname(__file__), 'regex_clr.dll'), 'rb') as f:
            assembly = binascii.hexlify(f.read()).decode('ascii')
        return assembly


@contextmanager
def use_master_connection(creation):
    if IS_DJANGO_16:
        test_db_name = creation._get_test_db_name()
        # swap in a master connection to allow add/drop of non-existant database
        old_wrapper = creation.connection
        try:
            creation.connection = creation._create_master_connection()
            # set the TEST_NAME for master connection so that it creates the
            # right one.
            creation.connection.settings_dict['TEST_NAME'] = test_db_name
            yield
        finally:
            creation.connection = old_wrapper
    else:
        # Django 1.7 uses the master connection already
        yield
