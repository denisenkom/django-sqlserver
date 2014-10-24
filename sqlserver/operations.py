from __future__ import absolute_import, unicode_literals

import datetime
import django
import django.core.exceptions
from django.conf import settings
from django.db.backends import BaseDatabaseOperations

try:
    from django.utils.encoding import smart_text
except:
    from django.utils.encoding import smart_unicode as smart_text

try:
    import pytz
except ImportError:
    pytz = None

from django.utils import six, timezone

from . import fields as mssql_fields

def total_seconds(td):
    if hasattr(td, 'total_seconds'):
        return td.total_seconds()
    else:
        return td.days * 24 * 60 * 60 + td.seconds


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "sqlserver.compiler"

    _convert_values_map = {
        # custom fields
        'DateTimeOffsetField':  mssql_fields.DateTimeOffsetField(),
        'LegacyDateField':      mssql_fields.LegacyDateField(),
        'LegacyDateTimeField':  mssql_fields.LegacyDateTimeField(),
        'LegacyTimeField':      mssql_fields.LegacyTimeField(),
        'NewDateField':         mssql_fields.DateField(),
        'NewDateTimeField':     mssql_fields.DateTimeField(),
        'NewTimeField':         mssql_fields.TimeField(),
    }

    # map of sql_function: (new sql_function, new sql_template )
    # If sql_template is None, it will not be overridden.
    _sql_function_overrides = {
        'STDDEV_SAMP': ('STDEV', None),
        'STDDEV_POP': ('STDEVP', None),
        'VAR_SAMP': ('VAR', None),
        'VAR_POP': ('VARP', None),
    }

    def __init__(self, *args, **kwargs):
        super(DatabaseOperations, self).__init__(*args, **kwargs)

        if self.connection.use_legacy_date_fields:
            self.value_to_db_datetime = self._legacy_value_to_db_datetime
            self.value_to_db_time = self._legacy_value_to_db_time

            self._convert_values_map.update({
                'DateField':        self._convert_values_map['LegacyDateField'],
                'DateTimeField':    self._convert_values_map['LegacyDateTimeField'],
                'TimeField':        self._convert_values_map['LegacyTimeField'],
            })
        else:
            self.value_to_db_datetime = self._new_value_to_db_datetime
            self.value_to_db_time = self._new_value_to_db_time

            self._convert_values_map.update({
                'DateField':        self._convert_values_map['NewDateField'],
                'DateTimeField':    self._convert_values_map['NewDateTimeField'],
                'TimeField':        self._convert_values_map['NewTimeField'],
            })

        if self.connection.cast_avg_to_float:
            # Need to cast as float to avoid truncating to an int
            self._sql_function_overrides['AVG'] = ('AVG', '%(function)s(CAST(%(field)s AS FLOAT))')

    def cache_key_culling_sql(self):
        return """
            SELECT [cache_key]
              FROM (SELECT [cache_key], ROW_NUMBER() OVER (ORDER BY [cache_key]) AS [rank] FROM %s) AS [RankedCache]
             WHERE [rank] = %%s + 1
        """

    def date_extract_sql(self, lookup_type, field_name):
        field_name = self.quote_name(field_name)
        if lookup_type == 'week_day':
            lookup_type = 'weekday'
        return 'DATEPART(%s, %s)' % (lookup_type, field_name)

    def date_interval_sql(self, sql, connector, timedelta):
        """
        implements the interval functionality for expressions
        format for SQL Server.
        """
        sign = 1 if connector == '+' else -1
        if timedelta.seconds or timedelta.microseconds:
            # assume the underlying datatype supports seconds/microseconds
            seconds = ((timedelta.days * 86400) + timedelta.seconds) * sign
            out = sql
            if seconds:
                out = 'DATEADD(SECOND, {0}, {1})'.format(seconds, sql)
            if timedelta.microseconds:
                # DATEADD with datetime doesn't support ms, must cast up
                out = 'DATEADD(MICROSECOND, {ms}, CAST({sql} as datetime2))'.format(
                    ms=timedelta.microseconds * sign,
                    sql=out,
                )
        else:
            # Only days in the delta, assume underlying datatype can DATEADD with days
            out = 'DATEADD(DAY, {0}, {1})'.format(timedelta.days * sign, sql)
        return out

    def date_trunc_sql(self, lookup_type, field_name):
        return "DATEADD(%s, DATEDIFF(%s, 0, %s), 0)" % (lookup_type, lookup_type, field_name)

    def _switch_tz_offset_sql(self, field_name, tzname):
        """
        Returns the SQL that will convert field_name to UTC from tzname.
        """
        field_name = self.quote_name(field_name)
        if settings.USE_TZ:
            if pytz is None:
                from django.core.exceptions import ImproperlyConfigured
                raise ImproperlyConfigured("This query requires pytz, "
                                           "but it isn't installed.")
            tz = pytz.timezone(tzname)
            td = tz.utcoffset(datetime.datetime(2000, 1, 1))

            total_minutes = total_seconds(td) // 60
            hours, minutes = divmod(total_minutes, 60)
            tzoffset = "%+03d:%02d" % (hours, minutes)
            field_name =\
                "CAST(SWITCHOFFSET(TODATETIMEOFFSET(%s, '+00:00'), '%s') AS DATETIME2)" % (field_name, tzoffset)
        return field_name

    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        """
        Given a lookup_type of 'year', 'month', 'day', 'hour', 'minute' or
        'second', returns the SQL that extracts a value from the given
        datetime field field_name, and a tuple of parameters.
        """
        if lookup_type == 'week_day':
            lookup_type = 'weekday'
        return 'DATEPART({0}, {1})'.format(
            lookup_type,
            self._switch_tz_offset_sql(field_name, tzname),
        ), []

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        """
        Given a lookup_type of 'year', 'month', 'day', 'hour', 'minute' or
        'second', returns the SQL that truncates the given datetime field
        field_name to a datetime object with only the given specificity, and
        a tuple of parameters.
        """
        field_name = self._switch_tz_offset_sql(field_name, tzname)
        reference_date = '0' # 1900-01-01
        if lookup_type in ['minute', 'second']:
            # Prevent DATEDIFF overflow by using the first day of the year as
            # the reference point. Only using for minute and second to avoid any
            # potential performance hit for queries against very large datasets.
            reference_date = "CONVERT(datetime2, CONVERT(char(4), {field_name}, 112) + '0101', 112)".format(
                field_name=field_name,
            )
        sql = "DATEADD({lookup}, DATEDIFF({lookup}, {reference_date}, {field_name}), {reference_date})".format(
            lookup=lookup_type,
            field_name=field_name,
            reference_date=reference_date,
        )
        return sql, []

    def last_insert_id(self, cursor, table_name, pk_name):
        """
        Fetch the last inserted ID by executing another query.
        """
        # IDENT_CURRENT   returns the last identity value generated for a
        #                 specific table in any session and any scope.
        # http://msdn.microsoft.com/en-us/library/ms175098.aspx
        cursor.execute("SELECT CAST(IDENT_CURRENT(%s) as bigint)", [self.quote_name(table_name)])
        return cursor.fetchone()[0]

    def return_insert_id(self):
        """
        MSSQL implements the RETURNING SQL standard extension differently from
        the core database backends and this function is essentially a no-op.
        The SQL is altered in the SQLInsertCompiler to add the necessary OUTPUT
        clause.
        """
        if django.VERSION[0] == 1 and django.VERSION[1] < 5:
            # This gets around inflexibility of SQLInsertCompiler's need to
            # append an SQL fragment at the end of the insert query, which also must
            # expect the full quoted table and column name.
            return ('/* %s */', '')

        # Django #19096 - As of Django 1.5, can return None, None to bypass the
        # core's SQL mangling.
        return (None, None)

    def no_limit_value(self):
        return None

    def prep_for_like_query(self, x):
        """Prepares a value for use in a LIKE query."""
        return (
            smart_text(x).
                replace("\\", "\\\\").
                replace("%", "\%").
                replace("_", "\_").
                replace("[", "\[").
                replace("]", "\]")
        )

    def quote_name(self, name):
        if name.startswith('[') and name.endswith(']'):
            return name # already quoted
        return '[%s]' % name

    def random_function_sql(self):
        return 'NEWID()'

    def regex_lookup(self, lookup_type):
        # Case sensitivity
        match_option = {'iregex': 0, 'regex': 1}[lookup_type]
        return "dbo.REGEXP_LIKE(%%s, %%s, %s)=1" % (match_option,)

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        """
        Returns a list of SQL statements required to remove all data from
        the given database tables (without actually removing the tables
        themselves).

        The `style` argument is a Style object as returned by either
        color_style() or no_style() in django.core.management.color.

        Originally taken from django-pyodbc project.
        """
        if not tables:
            return list()

        # Cannot use TRUNCATE on tables that are referenced by a FOREIGN KEY; use DELETE instead.
        # (which is slow)

        with self.connection.cursor() as cursor:
            # Try to minimize the risks of the braindeaded inconsistency in
            # DBCC CHEKIDENT(table, RESEED, n) behavior.
            seqs = []
            for seq in sequences:
                cursor.execute("SELECT COUNT(*) FROM %s" % self.quote_name(seq["table"]))
                rowcnt = cursor.fetchone()[0]
                elem = dict()

                if rowcnt:
                    elem['start_id'] = 0
                else:
                    elem['start_id'] = 1

                elem.update(seq)
                seqs.append(elem)

        sql_list = list()

        # Turn off constraints.
        sql_list.append('EXEC sp_MSforeachtable "ALTER TABLE ? NOCHECK CONSTRAINT all"')

        # Delete data from tables.
        sql_list.extend(
            ['%s %s %s;' % (
                style.SQL_KEYWORD('DELETE'),
                style.SQL_KEYWORD('FROM'),
                style.SQL_FIELD(self.quote_name(t))
            ) for t in tables]
        )

        # Reset the counters on each table.
        sql_list.extend(
            ['%s %s (%s, %s, %s) %s %s;' % (
                style.SQL_KEYWORD('DBCC'),
                style.SQL_KEYWORD('CHECKIDENT'),
                style.SQL_FIELD(self.quote_name(seq["table"])),
                style.SQL_KEYWORD('RESEED'),
                style.SQL_FIELD('%d' % seq['start_id']),
                style.SQL_KEYWORD('WITH'),
                style.SQL_KEYWORD('NO_INFOMSGS'),
            ) for seq in seqs]
        )

        # Turn constraints back on.
        sql_list.append('EXEC sp_MSforeachtable "ALTER TABLE ? WITH NOCHECK CHECK CONSTRAINT all"')

        return sql_list

    def tablespace_sql(self, tablespace, inline=False):
        return "ON %s" % self.quote_name(tablespace)

    _isoformat_space = ' ' if six.PY3 else b' '

    def __to_truncated_datetime_string(self, value):
        """
        Format a datetime to a internationalize string parsable by either a
        'datetime' or 'datetime2'.
        """
        if isinstance(value, datetime.datetime):
            # Strip '-' so SQL Server parses as YYYYMMDD for all languages/formats
            val = value.isoformat(self._isoformat_space).replace('-', '')
            if value.microsecond:
                # truncate to millisecond so SQL's 'datetime' can parse it
                idx = val.rindex('.')
                val = val[:idx + 4] + val[idx + 7:]
            return val
        raise TypeError("'value' must be a date or datetime")

    def _legacy_value_to_db_datetime(self, value):
        if value is None or isinstance(value, six.string_types):
            return value

        if timezone.is_aware(value):# and not self.connection.features.supports_timezones:
            if getattr(settings, 'USE_TZ', False):
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            else:
                raise ValueError("SQL Server backend does not support timezone-aware datetimes.")

        val = self.__to_truncated_datetime_string(value)
        return val

    def _new_value_to_db_datetime(self, value):
        if value is None or isinstance(value, six.string_types):
            return value

        if timezone.is_aware(value):# and not self.connection.features.supports_timezones:
            if getattr(settings, 'USE_TZ', False):
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            else:
                raise ValueError("SQL Server backend does not support timezone-aware datetimes.")
        return value

    def _legacy_value_to_db_time(self, value):
        if value is None or isinstance(value, six.string_types):
            return value

        if timezone.is_aware(value):
            if not getattr(settings, 'USE_TZ', False) and hasattr(value, 'astimezone'):
                value = timezone.make_naive(value, timezone.utc)
            else:
                raise ValueError("SQL Server backend does not support timezone-aware times.")

        val = value.isoformat()
        if value.microsecond:
            # truncate to millisecond so SQL's 'datetime' can parse it
            idx = val.rindex('.')
            val = val[:idx + 4] + val[idx + 7:]
        return val

    def _new_value_to_db_time(self, value):
        if value is None or isinstance(value, six.string_types):
            return value

        if timezone.is_aware(value):
            if not getattr(settings, 'USE_TZ', False) and hasattr(value, 'astimezone'):
                value = timezone.make_naive(value, timezone.utc)
            else:
                raise ValueError("SQL Server backend does not support timezone-aware times.")
        return value

    def value_to_db_decimal(self, value, max_digits, decimal_places):
        if value is None or value == '':
            return None
        return value # Should be a decimal type (or string)

    def year_lookup_bounds_for_date_field(self, value):
        """
        Returns a two-elements list with the lower and upper bound to be used
        with a BETWEEN operator to query a DateField value using a year
        lookup.

        `value` is an int, containing the looked-up year.
        """
        first = self.value_to_db_date(datetime.date(value, 1, 1))
        second = self.value_to_db_date(datetime.date(value, 12, 31))
        return [first, second]

    def year_lookup_bounds_for_datetime_field(self, value):
        """
        Returns a two-elements list with the lower and upper bound to be used
        with a BETWEEN operator to query a field value using a year lookup

        `value` is an int, containing the looked-up year.
        """
        first = datetime.datetime(value, 1, 1)
        ms = 997000 if self.connection.use_legacy_date_fields else 999999
        second = datetime.datetime(value, 12, 31, 23, 59, 59, ms)
        if settings.USE_TZ:
            tz = timezone.get_current_timezone()
            first = timezone.make_aware(first, tz)
            second = timezone.make_aware(second, tz)
        return [self.value_to_db_datetime(first), self.value_to_db_datetime(second)]

    def convert_values(self, value, field):
        """
        MSSQL needs help with date fields that might come out as strings.
        """
        if field:
            internal_type = field.get_internal_type()
            if internal_type in self._convert_values_map:
                value = self._convert_values_map[internal_type].to_python(value)
            else:
                value = super(DatabaseOperations, self).convert_values(value, field)
        return value

    def bulk_insert_sql(self, fields, num_values):
        """
        Format the SQL for bulk insert
        """
        items_sql = "(%s)" % ", ".join(["%s"] * len(fields))
        return "VALUES " + ", ".join([items_sql] * num_values)

    def max_name_length(self):
        """
        MSSQL supports identifier names up to 128
        """
        return 128

    def _supports_stddev(self):
        """
        Work around for django ticket #18334.
        This backend supports StdDev and the SQLCompilers will remap to
        the correct function names.
        """
        return True

    def enable_identity_insert(self, table):
        """
        Backends can implement as needed to enable inserts in to
        the identity column.

        Should return True if identity inserts have been enabled.
        """
        if table:
            cursor = self.connection.cursor()
            cursor.execute('SET IDENTITY_INSERT {0} ON'.format(
                self.connection.ops.quote_name(table)
            ))
            return True
        return False

    def disable_identity_insert(self, table):
        """
        Backends can implement as needed to disable inserts in to
        the identity column.

        Should return True if identity inserts have been disabled.
        """
        if table:
            cursor = self.connection.cursor()
            cursor.execute('SET IDENTITY_INSERT {0} OFF'.format(
                self.connection.ops.quote_name(table)
            ))
            return True
        return False

    def savepoint_create_sql(self, sid):
        return "SAVE TRANSACTION {0}".format(self.quote_name(sid))

    def savepoint_rollback_sql(self, sid):
        return "ROLLBACK TRANSACTION {0}".format(self.quote_name(sid))

    def combine_expression(self, connector, sub_expressions):
        """
        MSSQL requires special cases for ^ operators in query expressions
        """
        if connector == '^':
            return 'POWER(%s)' % ','.join(sub_expressions)
        return super(DatabaseOperations, self).combine_expression(connector, sub_expressions)


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
