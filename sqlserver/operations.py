from __future__ import absolute_import, unicode_literals

import sqlserver_ado.operations


class DatabaseOperations(sqlserver_ado.operations.DatabaseOperations):
    compiler_module = "sqlserver.compiler"

    def bulk_batch_size(self, fields, objs):
        """
        Returns the maximum allowed batch size for the backend. The fields
        are the fields going to be inserted in the batch, the objs contains
        all the objects to be inserted.
        """
        return min(len(objs), 2100 // len(fields), 1000)

    def for_update_sql(self, nowait=False):
        if nowait:
            return "WITH (ROWLOCK, UPDLOCK, NOWAIT)"
        else:
            return "WITH (ROWLOCK, UPDLOCK)"
