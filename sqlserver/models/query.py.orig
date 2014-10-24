from __future__ import unicode_literals
from django.db import connections, router
from django.db.models import sql
from django.db.models.query import RawQuerySet
from django.db.models.query_utils import deferred_class_factory, InvalidQuery

from sqlserver.ado.dbapi import FetchFailedError

__all__ = [
    'RawStoredProcedureQuery',
    'RawStoredProcedureQuerySet',
]

class RawStoredProcedureQuery(sql.RawQuery):
    """
    A single raw SQL stored procedure query
    """
    def clone(self, using):
        return RawStoredProcedureQuery(self.sql, using, params=self.params)

    def __repr__(self):
        return "<RawStoredProcedureQuery: %r %r>" % (self.sql, self.params)

    def _execute_query(self):
        """
        Execute the stored procedure using callproc, instead of execute.
        """
        self.cursor = connections[self.using].cursor()
        self.cursor.callproc(self.sql, self.params)


class RawStoredProcedureQuerySet(RawQuerySet):
    """
    Provides an iterator which converts the results of raw SQL queries into
    annotated model instances.

    raw_query should only be the name of the stored procedure.
    """
    def __init__(self, raw_query, model=None, query=None, params=None, translations=None, using=None):
        self.raw_query = raw_query
        self.model = model
        self._db = using
        self.query = query or RawStoredProcedureQuery(sql=raw_query, using=self.db, params=params)
        self.params = params or ()
        self.translations = translations or {}

    def __iter__(self):
        try:
            for x in super(RawStoredProcedureQuerySet, self).__iter__():
                yield x
        except FetchFailedError:
            # Stored procedure didn't return a record set
            pass

    def __repr__(self):
        return "<RawStoredProcedureQuerySet: %r %r>" % (self.raw_query, self.params)

    @property
    def columns(self):
        """
        A list of model field names in the order they'll appear in the
        query results.
        """
        if not hasattr(self, '_columns'):
            try:
                self._columns = self.query.get_columns()
            except TypeError:
                # "'NoneType' object is not iterable" thrown when stored procedure
                # doesn't return a result set.
                # no result means no column names, so grab them from the model
                self._columns = [self.model._meta.pk.db_column] #[x.db_column for x in self.model._meta.fields]

            # Adjust any column names which don't match field names
            for (query_name, model_name) in self.translations.items():
                try:
                    index = self._columns.index(query_name)
                    self._columns[index] = model_name
                except ValueError:
                    # Ignore translations for non-existant column names
                    pass

        return self._columns
