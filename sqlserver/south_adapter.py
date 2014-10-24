from __future__ import unicode_literals

from django.db import connection
from django.db.models.fields import BooleanField
from south.db import generic


class DatabaseOperations(generic.DatabaseOperations):
    """
    django-mssql (sql_server.mssql) implementation of database operations.
    """

    add_column_string = 'ALTER TABLE %s ADD %s;'
    alter_string_set_type = 'ALTER COLUMN %(column)s %(type)s'
    allows_combined_alters = False
    delete_column_string = 'ALTER TABLE %s DROP COLUMN %s;'

    def callproc(self, procname, params=None):
        """Call a stored procedure with the given parameter values"""
        with connection.cursor() as cursor:
            cursor.callproc(procname, params)

    def create_table(self, table_name, fields):
        # Tweak stuff as needed
        for name, f in fields:
            if isinstance(f, BooleanField):
                if f.default:
                    f.default = 1
                if not f.default:
                    f.default = 0

        # Run
        generic.DatabaseOperations.create_table(self, table_name, fields)

    def rename_column(self, table_name, old, new):
        """
        Renames the column 'old' from the table 'table_name' to 'new'.
        """
        # intentionally not quoting names
        self.callproc('sp_rename', (table_name + '.' + old, new, 'COLUMN'))
