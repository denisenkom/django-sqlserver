from ..introspection import BaseSqlDatabaseIntrospection
import _mssql

class DatabaseIntrospection(BaseSqlDatabaseIntrospection):
    data_types_reverse = {
        'AUTO_FIELD_MARKER': 'AutoField',
        _mssql.STRING: 'CharField',
        _mssql.NUMBER: 'IntegerField',
        _mssql.DECIMAL: 'DecimalField',
        _mssql.DATETIME: 'DateTimeField',
    }
