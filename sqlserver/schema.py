import binascii
import datetime
from django.utils import six

from .base_schema import BaseDatabaseSchemaEditor, logger


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_rename_table = "sp_rename '%(old_table)s', '%(new_table)s'"
    sql_delete_table = "DROP TABLE %(table)s"

    sql_create_column = "ALTER TABLE %(table)s ADD %(column)s %(definition)s"
    sql_alter_column_type = "ALTER COLUMN %(column)s %(type)s"
    sql_alter_column_null = "ALTER COLUMN %(column)s %(type)s NULL"
    sql_alter_column_not_null = "ALTER COLUMN %(column)s %(type)s NOT NULL"
    sql_alter_column_default = "ALTER COLUMN %(column)s ADD CONSTRAINT %(constraint_name)s DEFAULT %(default)s"
    sql_alter_column_default = "ADD CONSTRAINT %(constraint_name)s DEFAULT %(default)s FOR %(column)s"
    sql_alter_column_no_default = "ALTER COLUMN %(column)s DROP CONSTRAINT %(constraint_name)s"
    sql_delete_column = "ALTER TABLE %(table)s DROP COLUMN %(column)s"
    sql_rename_column = "sp_rename '%(table)s.%(old_column)s', '%(new_column)s', 'COLUMN'"

    sql_create_fk = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s) REFERENCES %(to_table)s (%(to_column)s)"

    sql_delete_index = "DROP INDEX %(name)s ON %(table)s"

    _sql_drop_inbound_foreign_keys = '''
DECLARE @sql nvarchar(max)
WHILE 1=1
BEGIN
    SELECT TOP 1
        @sql = N'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + N'].[' +
        OBJECT_NAME(parent_object_id) +'] DROP CONSTRAINT [' + name + N']'
    FROM sys.foreign_keys
    WHERE referenced_object_id = object_id(%s)
    IF @@ROWCOUNT = 0 BREAK
    EXEC (@sql)
END'''

    _sql_drop_primary_key = '''
DECLARE @sql nvarchar(max)
WHILE 1=1
BEGIN
    SELECT TOP 1
        @sql = N'ALTER TABLE [' + CONSTRAINT_SCHEMA + N'].[' + TABLE_NAME +
        N'] DROP CONSTRAINT [' + CONSTRAINT_NAME+ N']'
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu ON tc.CONSTRAINT_NAME = ccu.Constraint_name
    WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' AND TABLE_NAME LIKE %s AND COLUMN_NAME = %s
    IF @@ROWCOUNT = 0 BREAK
    EXEC (@sql)
END'''

    def alter_db_table(self, model, old_db_table, new_db_table):
        # sp_rename requires that objects not be quoted because they are string literals
        self.execute(self.sql_rename_table % {
            "old_table": old_db_table,
            "new_table": new_db_table,
        })

    def delete_model(self, model):
        # Drop all inbound FKs before dropping table
        self.execute(self._sql_drop_inbound_foreign_keys, [model._meta.db_table])
        super(DatabaseSchemaEditor, self).delete_model(model)

    def delete_db_column(self, model, column):
        # drop all of the column constraints to avoid the database blocking the column removal
        with self.connection.cursor() as cursor:
            constraints = self.connection.introspection.get_constraints(cursor, model._meta.db_table)
            for name, constraint in six.iteritems(constraints):
                if column in constraint['columns']:
                    sql = 'ALTER TABLE %(table)s DROP CONSTRAINT [%(constraint)s]' % {
                        'table': model._meta.db_table,
                        'constraint': name,
                    }
                    cursor.execute(sql)
        super(DatabaseSchemaEditor, self).delete_db_column(model, column)

    def rename_db_column(self, model, old_db_column, new_db_column, new_type):
        """
        Renames a column on a table.
        """
        self.execute(self.sql_rename_column % {
            "table": self.quote_name(model._meta.db_table),
            "old_column": self.quote_name(old_db_column),
            "new_column": new_db_column,  # not quoting because it's a string literal
            "type": new_type,
        })

    def __remove_identity_from_column(self, model, column, old_type, new_type):
        """
        Remove IDENTITY from a column. This is done by creating a new column and
        swapping in the values.
        """
        # removing identity from column
        args = {
            'table': model._meta.db_table,
            'column': column,
            'tmp_column': 'mssql_tmp_%s' % column,
            'type': new_type,
        }
        sql = []

        try:
            pk_constraint_name = self._constraint_names(model, primary_key=True)[0]
            # drop the existing primary key to allow drop of column later
            sql.append(self._delete_db_constraint_sql(model, pk_constraint_name, 'pk'))
            args['type'] += ' NOT NULL'  # pkey cannot be null
        except IndexError:  # no existing primary key
            pk_constraint_name = None

        # rename existing column to tmp name
        sql.append(("exec sp_rename %s, %s, 'COLUMN'", [
            '%s.%s' % (model._meta.db_table, args['column']),
            args['tmp_column'],
        ]))
        # create new column of type
        sql.append(("ALTER TABLE [%(table)s] ADD [%(column)s] %(type)s" % args, []))
        # copy identity values from old column
        sql.append(("UPDATE [%(table)s] SET [%(column)s] = [%(tmp_column)s]" % args, []))
        if pk_constraint_name:
            # create a primary key because alter_field expects one
            sql.append(self._create_db_constraint_sql(model, args['column'], 'pk'))

        # drop old column
        sql.append(("ALTER TABLE [%(table)s] DROP COLUMN [%(tmp_column)s]" % args, []))
        return [([], [])], sql

    def __add_identity_to_column(self, model, column, old_type, new_type):
        """
        Add IDENTITY to a column.
        """
        # To do this properly, we'd need to create a temporary table with the
        # new schema, copy the data, drop all inbound foreign keys to old table,
        # swap in the temp table, and finally rebuild all inbound foreign keys.
        raise NotImplementedError(
            "django-mssql doesn't support adding an IDENTITY column to a table."
        )

    def _alter_db_column_sql(self, model, column, alteration=None, values={}, fragment=False, params=None):
        if alteration == 'type':
            new_type = values.get('type', '').lower()
            old_type = values.get('old_type', '').lower()
            if 'identity' in old_type and 'identity' not in new_type:
                return self.__remove_identity_from_column(model, column, old_type, new_type)
            elif 'identity' not in old_type and 'identity' in new_type:
                return self.__add_identity_to_column(model, column, old_type, new_type)

        if alteration == 'default':
            # remove old default constraint
            remove_actions = self._alter_db_column_sql(model, column, alteration='no_default', values=values,
                fragment=fragment, params=params)
            # now add the new one
            actions = super(DatabaseSchemaEditor, self)._alter_db_column_sql(model, column, alteration,
                values, fragment, params)
            return (
                remove_actions[0] + actions[0],  # sql
                remove_actions[1] + actions[1]   # params
            )
        if alteration == 'no_default':
            # only post_actions to delete the default constraint
            sql, params = self._drop_default_column(model, column)
            return [([], [])], [(sql, params)]
        else:
            return super(DatabaseSchemaEditor, self)._alter_db_column_sql(model, column, alteration,
                values, fragment, params)

    def _drop_default_column(self, model, column):
        """
        Drop the default constraint for a column on a model.
        """
        sql = '''
DECLARE @sql nvarchar(max)
WHILE 1=1
BEGIN
    SELECT TOP 1 @sql = N'ALTER TABLE %(table)s DROP CONSTRAINT [' + dc.NAME + N']'
    FROM sys.default_constraints dc
    JOIN sys.columns c
        ON c.default_object_id = dc.object_id
    WHERE
        dc.parent_object_id = OBJECT_ID(%%s)
    AND c.name = %%s
    IF @@ROWCOUNT = 0 BREAK
    EXEC (@sql)
END''' % {'table': model._meta.db_table}
        params = [model._meta.db_table, column]
        return sql, params

    def prepare_default(self, value):
        return "%s" % self.quote_value(value), []

    def quote_value(self, value):
        if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
            return "'%s'" % value
        elif isinstance(value, six.text_type):
            return "'%s'" % six.text_type(value).replace("\'", "\'\'")
        elif isinstance(value, bytes):
            return "0x" + binascii.hexlify(value)
        elif isinstance(value, bool):
            return "1" if value else "0"
        elif value is None:
            return "NULL"
        else:
            return six.text_type(value)

    # def execute(self, sql, params=[]):
    #     """
    #     Executes the given SQL statement, with optional parameters.
    #     """
    #     if not sql:
    #         return
    #     # Log the command we're running, then run it
    #     logger.debug("%s; (params %r)" % (sql, params))
    #     if self.collect_sql:
    #         c = (sql % tuple(map(self._quote_parameter, params))) + ";"
    #         print 'collected sql=', c
    #         self.collected_sql.append(c)

    #     else:
    #         print 'sql=', sql
    #         print 'params=', params
    #         # Get the cursor
    #         with self.connection.cursor() as cursor:
    #             cursor.execute(sql, params)
