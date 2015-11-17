from __future__ import absolute_import, unicode_literals

import sqlserver_ado.operations


class DatabaseOperations(sqlserver_ado.operations.DatabaseOperations):
    compiler_module = "sqlserver.compiler"

    def for_update_sql(self, nowait=False):
        if nowait:
            return "WITH (ROWLOCK, UPDLOCK, NOWAIT)"
        else:
            return "WITH (ROWLOCK, UPDLOCK)"
