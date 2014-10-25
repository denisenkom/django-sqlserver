from __future__ import absolute_import, unicode_literals

import django
from django.db.utils import DatabaseError
from django.db.transaction import TransactionManagementError
from django.db.models.sql import compiler
import re
import six
import sqlserver_ado.compiler

NEEDS_AGGREGATES_FIX = django.VERSION[:2] < (1, 7)

_re_order_limit_offset = re.compile(
    r'(?:ORDER BY\s+(.+?))?\s*(?:LIMIT\s+(\d+))?\s*(?:OFFSET\s+(\d+))?$')

_re_find_order_direction = re.compile(r'\s+(asc|desc)\s*$', re.IGNORECASE)

# Pattern to find the quoted column name at the end of a field specification
_re_pat_col = re.compile(r"\[([^\[]+)\]$")

# Pattern to find each of the parts of a column name (extra_select, table, field)
_re_pat_col_parts = re.compile(
    r'(?:' +
    r'(\([^\)]+\))\s+as\s+' +
    r'|(\[[^\[]+\])\.' +
    r')?' +
    r'\[([^\[]+)\]$',
    re.IGNORECASE
)

# Pattern to scan a column data type string and split the data type from any
# constraints or other included parts of a column definition. Based upon
# <column_definition> from http://msdn.microsoft.com/en-us/library/ms174979.aspx
_re_data_type_terminator = re.compile(
    r'\s*\b(?:' +
    r'filestream|collate|sparse|not|null|constraint|default|identity|rowguidcol' +
    r'|primary|unique|clustered|nonclustered|with|on|foreign|references|check' +
    ')',
    re.IGNORECASE,
)

# Pattern used in column aliasing to find sub-select placeholders
_re_col_placeholder = re.compile(r'\{_placeholder_(\d+)\}')


def _break(s, find):
    """Break a string s into the part before the substring to find,
    and the part including and after the substring."""
    i = s.find(find)
    return s[:i], s[i:]


def _get_order_limit_offset(sql):
    return _re_order_limit_offset.search(sql).groups()


def _remove_order_limit_offset(sql):
    return _re_order_limit_offset.sub('', sql).split(None, 1)[1]


class SQLCompiler(sqlserver_ado.compiler.SQLCompiler):
    def resolve_columns(self, row, fields=()):
        # If the results are sliced, the resultset will have an initial
        # "row number" column. Remove this column before the ORM sees it.
        if getattr(self, '_using_row_number', False):
            row = row[1:]
        return super(SQLCompiler, self).resolve_columns(row, fields=fields)

    if hasattr(compiler.SQLCompiler, 'compile'):
        def parent_compile(self, node):
            return super(SQLCompiler, self).compile(node)
    else:
        def parent_compile(self, node):
            return node.as_sql(self.quote_name_unless_alias, self.connection)


    def compile(self, node):
        """
        Added with Django 1.7 as a mechanism to evalute expressions
        """
        sql_function = getattr(node, 'sql_function', None)
        if sql_function and sql_function in self.connection.ops._sql_function_overrides:
            sql_function, sql_template = self.connection.ops._sql_function_overrides[sql_function]
            if sql_function:
                node.sql_function = sql_function
            if sql_template:
                node.sql_template = sql_template
        return self.parent_compile(node)

    def get_from_clause(self):
        """
        Returns a list of strings that are joined together to go after the
        "FROM" part of the query, as well as a list any extra parameters that
        need to be included. Sub-classes, can override this to create a
        from-clause via a "select".

        This should only be called after any SQL construction methods that
        might change the tables we need. This means the select columns,
        ordering and distinct must be done first.

        overriden to add WITH (LOCK) modifiers
        """
        result = []
        qn = self.quote_name_unless_alias
        qn2 = self.connection.ops.quote_name
        first = True
        from_params = []

        # MODIFIED, added with modifier
        with_modifier = ''
        if self.query.select_for_update and self.connection.features.has_select_for_update:
            if self.connection.get_autocommit():
                raise TransactionManagementError("mssql_select_for_update cannot be used outside of a transaction.")

            # If we've been asked for a NOWAIT query but the backend does not support it,
            # raise a DatabaseError otherwise we could get an unexpected deadlock.
            nowait = self.query.select_for_update_nowait
            if nowait and not self.connection.features.has_select_for_update_nowait:
                raise DatabaseError('NOWAIT is not supported on this database backend.')
            with_modifier = self.connection.ops.for_update_sql(nowait=nowait)
        # END MODIFIED

        for alias in self.query.tables:
            if not self.query.alias_refcount[alias]:
                continue
            try:
                name, alias, join_type, lhs, join_cols, _, join_field = self.query.alias_map[alias]
            except KeyError:
                # Extra tables can end up in self.tables, but not in the
                # alias_map if they aren't in a join. That's OK. We skip them.
                continue
            alias_str = '' if alias == name else (' %s' % alias)
            if join_type and not first:
                extra_cond = join_field.get_extra_restriction(
                    self.query.where_class, alias, lhs)
                if extra_cond:
                    extra_sql, extra_params = self.compile(extra_cond)
                    extra_sql = 'AND (%s)' % extra_sql
                    from_params.extend(extra_params)
                else:
                    extra_sql = ""
                # MODIFIED, added with modifier
                result.append('%s %s%s %s ON ('
                        % (join_type, qn(name), alias_str, with_modifier))
                # END MODIFIED
                for index, (lhs_col, rhs_col) in enumerate(join_cols):
                    if index != 0:
                        result.append(' AND ')
                    result.append('%s.%s = %s.%s' %
                    (qn(lhs), qn2(lhs_col), qn(alias), qn2(rhs_col)))
                result.append('%s)' % extra_sql)
            else:
                connector = '' if first else ', '
                # MODIFIED, added with modifier
                result.append('%s%s%s %s' % (connector, qn(name), alias_str, with_modifier))
                # END MODIFIED
            first = False
        for t in self.query.extra_tables:
            alias, unused = self.query.table_alias(t)
            # Only add the alias if it's not already present (the table_alias()
            # calls increments the refcount, so an alias refcount of one means
            # this is the only reference.
            if alias not in self.query.alias_map or self.query.alias_refcount[alias] == 1:
                connector = '' if first else ', '
                result.append('%s%s' % (connector, qn(alias)))
                first = False
        return result, from_params

    def _as_sql(self, with_limits=True, with_col_aliases=False):
        """
        Creates the SQL for this query. Returns the SQL string and list of
        parameters.

        If 'with_limits' is False, any limit/offset information is not included
        in the query.
        """
        if with_limits and self.query.low_mark == self.query.high_mark:
            return '', ()

        self.pre_sql_setup()
        # After executing the query, we must get rid of any joins the query
        # setup created. So, take note of alias counts before the query ran.
        # However we do not want to get rid of stuff done in pre_sql_setup(),
        # as the pre_sql_setup will modify query state in a way that forbids
        # another run of it.
        self.refcounts_before = self.query.alias_refcount.copy()
        out_cols, s_params = self.get_columns(with_col_aliases)
        ordering, o_params, ordering_group_by = self.get_ordering()

        distinct_fields = self.get_distinct()

        # This must come after 'select', 'ordering' and 'distinct' -- see
        # docstring of get_from_clause() for details.
        from_, f_params = self.get_from_clause()

        where, w_params = self.compile(self.query.where)
        having, h_params = self.compile(self.query.having)
        if hasattr(self.query.having, 'get_group_by_cols'):  # Django 1.7
            having_group_by = self.query.having.get_group_by_cols()
        else:
            having_group_by = self.query.having.get_cols()
        params = []
        for val in six.itervalues(self.query.extra_select):
            params.extend(val[1])

        result = ['SELECT']

        if self.query.distinct:
            result.append(self.connection.ops.distinct_sql(distinct_fields))

        result.append(', '.join(out_cols + self.ordering_aliases))
        params.extend(s_params)
        params.extend(self.ordering_params)

        result.append('FROM')
        result.extend(from_)
        params.extend(f_params)

        if where:
            result.append('WHERE %s' % where)
            params.extend(w_params)

        grouping, gb_params = self.get_grouping(having_group_by, ordering_group_by)
        if grouping:
            if distinct_fields:
                raise NotImplementedError(
                    "annotate() + distinct(fields) not implemented.")
            if not ordering:
                ordering = self.connection.ops.force_no_ordering()
            result.append('GROUP BY %s' % ', '.join(grouping))
            params.extend(gb_params)

        if having:
            result.append('HAVING %s' % having)
            params.extend(h_params)

        if ordering:
            result.append('ORDER BY %s' % ', '.join(ordering))
            params.extend(o_params)

        # Finally do cleanup - get rid of the joins we created above.
        self.query.reset_refcounts(self.refcounts_before)

        # MODIFIED, removed with_limits processing,
        # select_for_update moved to get_from_clause
        # END MODIFIED

        # Finally do cleanup - get rid of the joins we created above.
        self.query.reset_refcounts(self.refcounts_before)

        return ' '.join(result), tuple(params)

    def as_sql(self, with_limits=True, with_col_aliases=False):
        # Django #12192 - Don't execute any DB query when QS slicing results in limit 0
        if with_limits and self.query.low_mark == self.query.high_mark:
            return '', ()

        if NEEDS_AGGREGATES_FIX:
            # Django 1.7+ provides SQLCompiler.compile as a hook
            self._fix_aggregates()

        self._using_row_number = False

        # Get out of the way if we're not a select query or there's no limiting involved.
        has_limit_offset = with_limits and (self.query.low_mark or self.query.high_mark is not None)
        if not has_limit_offset:
            # The ORDER BY clause is invalid in views, inline functions,
            # derived tables, subqueries, and common table expressions,
            # unless TOP or FOR XML is also specified.
            try:
                setattr(self.query, '_mssql_ordering_not_allowed', with_col_aliases)
                result = self._as_sql(
                    with_limits=False,
                    with_col_aliases=with_col_aliases,
                )
            finally:
                # remove in case query is every reused
                delattr(self.query, '_mssql_ordering_not_allowed')
            return result

        raw_sql, fields = self._as_sql(
            with_limits=False,
            with_col_aliases=with_col_aliases)

        # Check for high mark only and replace with "TOP"
        if self.query.high_mark is not None and not self.query.low_mark:
            _select = 'SELECT'
            if self.query.distinct:
                _select += ' DISTINCT'

            sql = re.sub(r'(?i)^{0}'.format(_select), '{0} TOP {1}'.format(_select, self.query.high_mark), raw_sql, 1)
            return sql, fields

        # Else we have limits; rewrite the query using ROW_NUMBER()
        self._using_row_number = True

        # Lop off ORDER... and the initial "SELECT"
        inner_select = _remove_order_limit_offset(raw_sql)
        outer_fields, inner_select = self._alias_columns(inner_select)

        order = _get_order_limit_offset(raw_sql)[0]

        qn = self.connection.ops.quote_name
        inner_table_name = qn('AAAA')

        outer_fields, inner_select, order = self._fix_slicing_order(outer_fields, inner_select, order, inner_table_name)

        # map a copy of outer_fields for injected subselect
        f = []
        for x in outer_fields.split(','):
            i = x.upper().find(' AS ')
            if i != -1:
                x = x[i+4:]
            if x.find('.') != -1:
                tbl, col = x.rsplit('.', 1)
            else:
                col = x
            f.append('{0}.{1}'.format(inner_table_name, col.strip()))

        # inject a subselect to get around OVER requiring ORDER BY to come from FROM
        inner_select = '{fields} FROM ( SELECT {inner} ) AS {inner_as}'.format(
            fields=', '.join(f),
            inner=inner_select,
            inner_as=inner_table_name,
        )

        where_row_num = '{0} < _row_num'.format(self.query.low_mark)
        if self.query.high_mark:
            where_row_num += ' and _row_num <= {0}'.format(self.query.high_mark)

        sql = "SELECT _row_num, {outer} FROM ( SELECT ROW_NUMBER() OVER ( ORDER BY {order}) as _row_num, {inner}) as QQQ where {where}".format(
            outer=outer_fields,
            order=order,
            inner=inner_select,
            where=where_row_num,
        )

        return sql, fields

    def _fix_slicing_order(self, outer_fields, inner_select, order, inner_table_name):
        """
        Apply any necessary fixes to the outer_fields, inner_select, and order
        strings due to slicing.
        """
        # Using ROW_NUMBER requires an ordering
        if order is None:
            meta = self.query.get_meta()
            column = meta.pk.db_column or meta.pk.get_attname()
            order = '{0}.{1} ASC'.format(
                inner_table_name,
                self.connection.ops.quote_name(column),
            )
        else:
            alias_id = 0
            # remap order for injected subselect
            new_order = []
            for x in order.split(','):
                # find the ordering direction
                m = _re_find_order_direction.search(x)
                if m:
                    direction = m.groups()[0]
                else:
                    direction = 'ASC'
                # remove the ordering direction
                x = _re_find_order_direction.sub('', x)
                # remove any namespacing or table name from the column name
                col = x.rsplit('.', 1)[-1]
                # Is the ordering column missing from the inner select?
                # 'inner_select' contains the full query without the leading 'SELECT '.
                # It's possible that this can get a false hit if the ordering
                # column is used in the WHERE while not being in the SELECT. It's
                # not worth the complexity to properly handle that edge case.
                if x not in inner_select:
                    # Ordering requires the column to be selected by the inner select
                    alias_id += 1
                    # alias column name
                    col = '[{0}___o{1}]'.format(
                        col.strip('[]'),
                        alias_id,
                    )
                    # add alias to inner_select
                    inner_select = '({0}) AS {1}, {2}'.format(x, col, inner_select)
                new_order.append('{0}.{1} {2}'.format(inner_table_name, col, direction))

            order = ', '.join(new_order)
        return outer_fields, inner_select, order

    def _alias_columns(self, sql):
        """Return tuple of SELECT and FROM clauses, aliasing duplicate column names."""
        qn = self.connection.ops.quote_name

        outer = list()
        inner = list()
        names_seen = list()

        # replace all parens with placeholders
        paren_depth, paren_buf = 0, ['']
        parens, i = {}, 0
        for ch in sql:
            if ch == '(':
                i += 1
                paren_depth += 1
                paren_buf.append('')
            elif ch == ')':
                paren_depth -= 1
                key = '_placeholder_{0}'.format(i)
                buf = paren_buf.pop()

                # store the expanded paren string
                parens[key] = buf.format(**parens)
                paren_buf[paren_depth] += '({' + key + '})'
            else:
                paren_buf[paren_depth] += ch

        def _replace_sub(col):
            """Replace all placeholders with expanded values"""
            while _re_col_placeholder.search(col):
                col = col.format(**parens)
            return col

        temp_sql = ''.join(paren_buf)

        select_list, from_clause = _break(temp_sql, ' FROM [')

        for col in [x.strip() for x in select_list.split(',')]:
            match = _re_pat_col.search(col)
            if match:
                col_name = match.group(1)
                col_key = col_name.lower()

                if col_key in names_seen:
                    alias = qn('{0}___{1}'.format(col_name, names_seen.count(col_key)))
                    outer.append(alias)
                    inner.append('{0} as {1}'.format(_replace_sub(col), alias))
                else:
                    outer.append(qn(col_name))
                    inner.append(_replace_sub(col))

                names_seen.append(col_key)
            else:
                raise Exception('Unable to find a column name when parsing SQL: {0}'.format(col))

        return ', '.join(outer), ', '.join(inner) + from_clause.format(**parens)


class SQLInsertCompiler(sqlserver_ado.compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(sqlserver_ado.compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(sqlserver_ado.compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(sqlserver_ado.compiler.SQLAggregateCompiler, SQLCompiler):
    pass


class SQLDateCompiler(sqlserver_ado.compiler.SQLDateCompiler, SQLCompiler):
    pass


try:
    class SQLDateTimeCompiler(sqlserver_ado.compiler.SQLDateTimeCompiler, SQLCompiler):
        pass
except AttributeError:
    pass
