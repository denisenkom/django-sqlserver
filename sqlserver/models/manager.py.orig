from __future__ import unicode_literals
from django.db.models import Manager
from sqlserver.ado.models.query import RawStoredProcedureQuerySet

class RawStoredProcedureManager(Manager):
    """
    Adds raw_callproc, which behaves the same as Manager.raw, but relies upon
    stored procedure that returns a single result set.
    """
    def raw_callproc(self, proc_name, params=None, *args, **kwargs):
        """
        Execute a stored procedure that returns a single resultset that can be
        used to load the current Model. The return value from the stored
        procedure will be ignored.

        proc_name is expected to be properly quoted.
        """
        return RawStoredProcedureQuerySet(raw_query=proc_name, model=self.model, params=params, using=self._db,
            *args, **kwargs)
