from __future__ import absolute_import, unicode_literals
from ..introspection import BaseSqlDatabaseIntrospection

from . import ado_consts

AUTO_FIELD_MARKER = -1000
BIG_AUTO_FIELD_MARKER = -1001
MONEY_FIELD_MARKER = -1002

class DatabaseIntrospection(BaseSqlDatabaseIntrospection):
    def _get_table_field_type_map(self, cursor, table_name):
        """
        Return a dict mapping field name to data type. DB-API cursor description 
        interprets the date columns as chars.
        """
        cursor.execute('SELECT [COLUMN_NAME], [DATA_TYPE] FROM INFORMATION_SCHEMA.COLUMNS WHERE [TABLE_NAME] LIKE \'%s\'' % table_name)
        results = dict(cursor.fetchall())
        return results

    def _datatype_to_ado_type(self, datatype):
        """
        Map datatype name to ado type.
        """
        return {
            'bigint': ado_consts.adBigInt,
            'binary': ado_consts.adBinary,
            'bit': ado_consts.adBoolean,
            'char': ado_consts.adChar,
            'date': ado_consts.adDBDate,
            'datetime': ado_consts.adDBTimeStamp,
            'datetime2': ado_consts.adDBTimeStamp,
            'datetimeoffset': ado_consts.adDBTimeStamp,
            'decimal': ado_consts.adDecimal,
            'float': ado_consts.adDouble,
            'image': ado_consts.adVarBinary,
            'int': ado_consts.adInteger,
            'money': MONEY_FIELD_MARKER,
            'numeric': ado_consts.adNumeric,
            'nchar': ado_consts.adWChar,
            'ntext': ado_consts.adLongVarWChar,
            'nvarchar': ado_consts.adVarWChar,
            'smalldatetime': ado_consts.adDBTimeStamp,
            'smallint': ado_consts.adSmallInt,
            'smallmoney': MONEY_FIELD_MARKER,
            'text': ado_consts.adLongVarChar,
            'time': ado_consts.adDBTime,
            'tinyint': ado_consts.adTinyInt,
            'varbinary': ado_consts.adVarBinary,
            'varchar': ado_consts.adVarChar,
        }.get(datatype.lower(), None)

    def get_table_description(self, cursor, table_name, identity_check=True):
        """Return a description of the table, with DB-API cursor.description interface.

        The 'auto_check' parameter has been added to the function argspec.
        If set to True, the function will check each of the table's fields for the
        IDENTITY property (the IDENTITY property is the MSSQL equivalent to an AutoField).

        When a field is found with an IDENTITY property, it is given a custom field number
        of SQL_AUTOFIELD, which maps to the 'AutoField' value in the DATA_TYPES_REVERSE dict.
        """
        table_field_type_map = self._get_table_field_type_map(cursor, table_name)

        cursor.execute("SELECT * FROM [%s] where 1=0" % (table_name))
        columns = cursor.description

        items = list()
        for column in columns:
            column = list(column) # Convert tuple to list
            # fix data type
            column[1] = self._datatype_to_ado_type(table_field_type_map.get(column[0]))

            if identity_check and self._is_auto_field(cursor, table_name, column[0]):
                if column[1] == ado_consts.adBigInt:
                    column[1] = BIG_AUTO_FIELD_MARKER
                else:
                    column[1] = AUTO_FIELD_MARKER

            if column[1] == MONEY_FIELD_MARKER:
                # force decimal_places=4 to match data type. Cursor description thinks this column is a string
                column[5] = 4
            items.append(column)
        return items

    data_types_reverse = {
        AUTO_FIELD_MARKER: 'IntegerField',
        BIG_AUTO_FIELD_MARKER: 'sqlserver_ado.fields.BigAutoField',
        MONEY_FIELD_MARKER: 'DecimalField',
        ado_consts.adBoolean: 'BooleanField',
        ado_consts.adChar: 'CharField',
        ado_consts.adWChar: 'CharField',
        ado_consts.adDecimal: 'DecimalField',
        ado_consts.adNumeric: 'DecimalField',
        ado_consts.adDate: 'DateField',
        ado_consts.adDBDate: 'DateField',
        ado_consts.adDBTime: 'TimeField',
        ado_consts.adDBTimeStamp: 'DateTimeField',
        ado_consts.adDouble: 'FloatField',
        ado_consts.adSingle: 'FloatField',
        ado_consts.adInteger: 'IntegerField',
        ado_consts.adBigInt: 'BigIntegerField',
        ado_consts.adSmallInt: 'SmallIntegerField',
        ado_consts.adTinyInt: 'SmallIntegerField',
        ado_consts.adVarChar: 'CharField',
        ado_consts.adVarWChar: 'CharField',
        ado_consts.adLongVarWChar: 'TextField',
        ado_consts.adLongVarChar: 'TextField',
        ado_consts.adBinary: 'BinaryField',
        ado_consts.adVarBinary: 'BinaryField',
        ado_consts.adLongVarBinary: 'BinaryField',
        }
