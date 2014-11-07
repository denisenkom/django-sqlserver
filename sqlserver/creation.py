from __future__ import absolute_import, unicode_literals
from sqlserver_ado import creation


class DatabaseCreation(creation.DatabaseCreation):
    def sql_create_model(self, model, style, known_models=set()):
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

    def enable_clr(self):
        """ Enables clr for server if not already enabled

        This function will not fail if current user doesn't have
        permissions to enable clr, and clr is already enabled
        """
        with self._nodb_connection.cursor() as cursor:
            # check whether clr is enabled
            cursor.execute('''
            SELECT value FROM sys.configurations
            WHERE name = 'clr enabled'
            ''')
            res = cursor.fetchone()

            if not res or not res[0]:
                # if not enabled enable clr
                cursor.execute("sp_configure 'clr enabled', 1")
                cursor.execute("RECONFIGURE")

    def install_regex_clr(self, database_name):
        install_regex_sql = '''
USE {database_name};

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

        self.enable_clr()

        with self._nodb_connection.cursor() as cursor:
            for s in install_regex_sql:
                cursor.execute(s)

    def create_test_db(self, *args, **kwargs):
        # replace mssql's create_test_db with standard create_test_db
        # this removes mark_tests_as_expected_failure, which
        # prevented tests to work in client code
        # this code moved to tests/runtests.py
        super(creation.DatabaseCreation, self).create_test_db(*args, **kwargs)
