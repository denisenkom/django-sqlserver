import django
import sqlserver_ado.compiler


# re-export compiler classes from django-mssql
SQLCompiler = sqlserver_ado.compiler.SQLCompiler
SQLInsertCompiler = sqlserver_ado.compiler.SQLInsertCompiler
SQLDeleteCompiler = sqlserver_ado.compiler.SQLDeleteCompiler
SQLUpdateCompiler = sqlserver_ado.compiler.SQLUpdateCompiler
SQLAggregateCompiler = sqlserver_ado.compiler.SQLAggregateCompiler
try:
    SQLDateCompiler = sqlserver_ado.compiler.SQLDateCompiler
except AttributeError:
    pass

try:
    SQLDateTimeCompiler = sqlserver_ado.compiler.SQLDateTimeCompiler
except AttributeError:
    pass


if django.VERSION >= (1, 11, 0):
    # django 1.11 or newer

    # monkey patch django-mssql to support Django 1.11
    # can be removed once django-mssql begins to support Django 1.11
    def _django_11_mssql_monkeypatch_as_sql(self, with_limits=True, with_col_aliases=False):
        # Get out of the way if we're not a select query or there's no limiting involved.
        has_limit_offset = with_limits and (self.query.low_mark or self.query.high_mark is not None)
        try:
            if not has_limit_offset:
                # The ORDER BY clause is invalid in views, inline functions,
                # derived tables, subqueries, and common table expressions,
                # unless TOP or FOR XML is also specified.
                setattr(self.query, '_mssql_ordering_not_allowed', with_col_aliases)

            # let the base do its thing, but we'll handle limit/offset
            sql, fields = super(sqlserver_ado.compiler.SQLCompiler, self).as_sql(
                with_limits=False,
                with_col_aliases=with_col_aliases,
            )

            if has_limit_offset:
                if ' order by ' not in sql.lower():
                    # Must have an ORDER BY to slice using OFFSET/FETCH. If
                    # there is none, use the first column, which is typically a
                    # PK
                    sql += ' ORDER BY 1'
                sql += ' OFFSET %d ROWS' % (self.query.low_mark or 0)
                if self.query.high_mark is not None:
                    sql += ' FETCH NEXT %d ROWS ONLY' % (self.query.high_mark - self.query.low_mark)
        finally:
            if not has_limit_offset:
                # remove in case query is ever reused
                delattr(self.query, '_mssql_ordering_not_allowed')

        return sql, fields


    sqlserver_ado.compiler.SQLCompiler.as_sql = _django_11_mssql_monkeypatch_as_sql
