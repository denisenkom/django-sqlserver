from __future__ import absolute_import, unicode_literals

import sqlserver_ado.operations
import datetime


class DatabaseOperations(sqlserver_ado.operations.DatabaseOperations):
    compiler_module = "sqlserver.compiler"

    def for_update_sql(self, nowait=False, skip_locked=False):
        hints = ['ROWLOCK', 'UPDLOCK']
        if nowait:
            hints += ['NOWAIT']
        if skip_locked:
            hints += ['READPAST']
        return "WITH ({})".format(','.join(hints))

    def value_to_db_date(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            value = value.date()
        return value.isoformat()
