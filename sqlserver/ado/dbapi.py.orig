"""A DB API 2.0 interface to SQL Server for Django

Forked from: adodbapi v2.1
Copyright (C) 2002 Henrik Ekelund, version 2.1 by Vernon Cole
* http://adodbapi.sourceforge.net/
* http://sourceforge.net/projects/pywin32/

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

* Version 2.1D by Adam Vandenberg, forked for Django backend use.
  This module is a db-api 2 interface for ADO, but is Django & SQL Server.
  It won't work against other ADO sources (like Access.)

DB-API 2.0 specification: http://www.python.org/dev/peps/pep-0249/
"""
from __future__ import absolute_import, unicode_literals

import sys
import time
import datetime
import re

import decimal

from django.conf import settings
from django.db.utils import IntegrityError as DjangoIntegrityError, \
    DatabaseError as DjangoDatabaseError

from django.utils import six
from django.utils import timezone

from .ado_consts import *

# DB API default values
apilevel = '2.0'

# 1: Threads may share the module, but not connections.
threadsafety = 1

# The underlying ADO library expects parameters as '?', but this wrapper
# expects '%s' parameters. This wrapper takes care of the conversion.
paramstyle = 'format'

# Set defaultIsolationLevel on module level before creating the connection.
# It may be one of "adXact..." consts.
defaultIsolationLevel = adXactReadCommitted

# Set defaultCursorLocation on module level before creating the connection.
# It may be one of the "adUse..." consts.
defaultCursorLocation = adUseServer

# Used for COM to Python date conversions.
_ordinal_1899_12_31 = datetime.date(1899,12,31).toordinal()-1
_milliseconds_per_day = 24*60*60*1000


class MultiMap(object):
    def __init__(self, mapping, default=None):
        """Defines a mapping with multiple keys per value.

        mapping is a dict of: tuple(key, key, key...) => value
        """
        self.storage = dict()
        self.default = default

        for keys, value in six.iteritems(mapping):
            for key in keys:
                self.storage[key] = value

    def __getitem__(self, key):
        return self.storage.get(key, self.default)


def standardErrorHandler(connection, cursor, errorclass, errorvalue):
    err = (errorclass, errorvalue)
    if connection is not None:
        connection.messages.append(err)
    if cursor is not None:
        cursor.messages.append(err)
    raise errorclass(errorvalue)


class Error(Exception if six.PY3 else StandardError): pass
class Warning(Exception if six.PY3 else StandardError): pass

class InterfaceError(Error): pass

class DatabaseError(DjangoDatabaseError, Error): pass

class InternalError(DatabaseError): pass
class OperationalError(DatabaseError): pass
class ProgrammingError(DatabaseError): pass
class IntegrityError(DatabaseError, DjangoIntegrityError): pass
class DataError(DatabaseError): pass
class NotSupportedError(DatabaseError): pass

class FetchFailedError(Error):
    """
    Error is used by RawStoredProcedureQuerySet to determine when a fetch
    failed due to a connection being closed or there is no record set
    returned.
    """
    pass

class _DbType(object):
    def __init__(self,valuesTuple):
        self.values = valuesTuple

    def __eq__(self, other): return other in self.values
    def __ne__(self, other): return other not in self.values

_re_find_password = re.compile('(pwd|password)=[^;]*;', re.IGNORECASE)

def mask_connection_string_password(s, mask='******'):
    """
    Look for a connection string password in 's' and mask it.
    """
    return re.sub(_re_find_password, '\g<1>=%s;' % mask, s)

def connect(connection_string, timeout=30, use_transactions=None):
    """Connect to a database.

    connection_string -- An ADODB formatted connection string, see:
        http://www.connectionstrings.com/?carrier=sqlserver2005
    timeout -- A command timeout value, in seconds (default 30 seconds)
    """
    # Inner imports to make this module importable on non-Windows platforms.
    import pythoncom
    import win32com.client
    try:
        pythoncom.CoInitialize()
        c = win32com.client.Dispatch('ADODB.Connection')
        c.CommandTimeout = timeout
        c.ConnectionString = connection_string
        c.Open()
        if use_transactions is None:
            useTransactions = _use_transactions(c)
        else:
            useTransactions = use_transactions
        return Connection(c, useTransactions)
    except Exception as e:
        raise OperationalError(e,
            "Error opening connection: {0}".format(
                mask_connection_string_password(connection_string)
            )
        )

def _use_transactions(c):
    """Return True if the given ADODB.Connection supports transactions."""
    for prop in c.Properties:
        if prop.Name == 'Transaction DDL':
            return prop.Value > 0
    return False

def format_parameters(parameters, show_value=False):
    """Format a collection of ADO Command Parameters.

    Used by error reporting in _execute_command.
    """
    directions = {
        0: 'Unknown',
        1: 'Input',
        2: 'Output',
        3: 'In/Out',
        4: 'Return',
    }

    if show_value:
        desc = [
            "Name: %s, Dir.: %s, Type: %s, Size: %s, Value: \"%s\", Precision: %s, NumericScale: %s" %\
            (p.Name, directions[p.Direction], adTypeNames.get(p.Type, str(p.Type)+' (unknown type)'), p.Size, p.Value, p.Precision, p.NumericScale)
            for p in parameters ]
    else:
        desc = [
            "Name: %s, Dir.: %s, Type: %s, Size: %s, Precision: %s, NumericScale: %s" %\
            (p.Name, directions[p.Direction], adTypeNames.get(p.Type, str(p.Type)+' (unknown type)'), p.Size, p.Precision, p.NumericScale)
            for p in parameters ]

    return '[' + ', '.join(desc) + ']'

def format_decimal_as_string(value):
    """
    Convert a decimal.Decimal to a fixed point string. Code borrowed from
    Python's moneyfmt recipe.
    https://docs.python.org/2/library/decimal.html#recipes
    """
    sign, digits, exp = value.as_tuple()
    result = []
    digits = list(map(str, digits))
    build, next = result.append, digits.pop
    for i in range(-exp):
        build(next() if digits else '0')
    build('.')
    if not digits:
        build('0')
    while digits:
        build(next())
    if sign:
        build('-')
    return ''.join(reversed(result))

def _configure_parameter(p, value):
    """Configure the given ADO Parameter 'p' with the Python 'value'."""
    if p.Direction not in [adParamInput, adParamInputOutput, adParamUnknown]:
        return

    if isinstance(value, six.string_types):
        p.Value = value
        p.Size = len(value)

    elif isinstance(value, six.memoryview):
        p.Size = len(value)
        p.AppendChunk(value)

    elif isinstance(value, decimal.Decimal):
        p.Type = adBSTR
        p.Value = format_decimal_as_string(value)

    elif isinstance(value, datetime.datetime):
        p.Type = adBSTR
        if timezone.is_aware(value):
            value = timezone.make_naive(value, timezone.utc)
        # Strip '-' so SQL Server parses as YYYYMMDD for all languages/formats
        s = value.isoformat(' ' if six.PY3 else b' ').replace('-', '')
        p.Value = s
        p.Size = len(s)

    elif isinstance(value, datetime.time):
        p.Type = adBSTR
        s = value.isoformat()
        p.Value = s
        p.Size = len(s)

    else:
        # For any other type, set the value and let pythoncom do the right thing.
        p.Value = value

    # Use -1 instead of 0 for empty strings and buffers
    if p.Size == 0:
        p.Size = -1

class Connection(object):
    def __init__(self, adoConn, useTransactions=False):
        self.adoConn = adoConn
        self.errorhandler = None
        self.messages = []
        self.adoConn.CursorLocation = defaultCursorLocation
        self.supportsTransactions = useTransactions
        self.transaction_level = 0 # 0 == Not in a transaction, at the top level

        if self.supportsTransactions:
            self.adoConn.IsolationLevel = defaultIsolationLevel
            self.transaction_level = self.adoConn.BeginTrans() # Disables autocommit per DBPAI

    def set_autocommit(self, value):
        if self.supportsTransactions == (not value):
            return
        if self.supportsTransactions:
            self.transaction_level = self.adoConn.RollbackTrans() # Disables autocommit per DBPAI
        else:
            self.adoConn.IsolationLevel = defaultIsolationLevel
            self.transaction_level = self.adoConn.BeginTrans() # Disables autocommit per DBPAI
        self.supportsTransactions = not value

    def _raiseConnectionError(self, errorclass, errorvalue):
        eh = self.errorhandler
        if eh is None:
            eh = standardErrorHandler
        eh(self, None, errorclass, errorvalue)

    def _close_connection(self):
        """Close the underlying ADO Connection object, rolling back an active transaction if supported."""
        if self.supportsTransactions:
            self.transaction_level = self.adoConn.RollbackTrans()
        self.adoConn.Close()

    def close(self):
        """Close the database connection."""
        self.messages = []
        try:
            self._close_connection()
        except Exception as e:
            self._raiseConnectionError(InternalError, e)
        self.adoConn = None
         # Inner import to make this module importable on non-Windows platforms.
        import pythoncom
        pythoncom.CoUninitialize()

    def commit(self):
        """Commit a pending transaction to the database.

        Note that if the database supports an auto-commit feature, this must
        be initially off.
        """
        self.messages = []
        if not self.supportsTransactions:
            return

        try:
            self.transaction_level = self.adoConn.CommitTrans()
            if not(self.adoConn.Attributes & adXactCommitRetaining):
                #If attributes has adXactCommitRetaining it performs retaining commits that is,
                #calling CommitTrans automatically starts a new transaction. Not all providers support this.
                #If not, we will have to start a new transaction by this command:
                self.adoConn.BeginTrans()
        except Exception as e:
            self._raiseConnectionError(Error, e)

    def rollback(self):
        """Abort a pending transaction."""
        self.messages = []
        with self.cursor() as cursor:
            cursor.execute("select @@TRANCOUNT")
            trancount, = cursor.fetchone()
        if trancount == 0:
            return
        self.transaction_level = self.adoConn.RollbackTrans()
        if not(self.adoConn.Attributes & adXactAbortRetaining):
            #If attributes has adXactAbortRetaining it performs retaining aborts that is,
            #calling RollbackTrans automatically starts a new transaction. Not all providers support this.
            #If not, we will have to start a new transaction by this command:
            self.transaction_level = self.adoConn.BeginTrans()

    def cursor(self):
        """Return a new Cursor object using the current connection."""
        self.messages = []
        return Cursor(self)

    def printADOerrors(self):
        print('ADO Errors (%i):' % self.adoConn.Errors.Count)
        for e in self.adoConn.Errors:
            print('Description: %s' % e.Description)
            print('Error: %s %s ' % (e.Number, adoErrors.get(e.Number, "unknown")))
            if e.Number == ado_error_TIMEOUT:
                print('Timeout Error: Try using adodbpi.connect(constr,timeout=Nseconds)')
            print('Source: %s' % e.Source)
            print('NativeError: %s' % e.NativeError)
            print('SQL State: %s' % e.SQLState)

    def _suggest_error_class(self):
        """Introspect the current ADO Errors and determine an appropriate error class.

        Error.SQLState is a SQL-defined error condition, per the SQL specification:
        http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt

        The 23000 class of errors are integrity errors.
        Error 40002 is a transactional integrity error.
        """
        if self.adoConn is not None:
            for e in self.adoConn.Errors:
                state = str(e.SQLState)
                if state.startswith('23') or state=='40002':
                    return IntegrityError

        return DatabaseError

    def __del__(self):
        try:
            self._close_connection()
        except: pass
        self.adoConn = None


class Cursor(object):
##    This read-only attribute is a sequence of 7-item sequences.
##    Each of these sequences contains information describing one result column:
##        (name, type_code, display_size, internal_size, precision, scale, null_ok).
##    This attribute will be None for operations that do not return rows or if the
##    cursor has not had an operation invoked via the executeXXX() method yet.
##    The type_code can be interpreted by comparing it to the Type Objects specified in the section below.
    description = None

##    This read-only attribute specifies the number of rows that the last executeXXX() produced
##    (for DQL statements like select) or affected (for DML statements like update or insert).
##    The attribute is -1 in case no executeXXX() has been performed on the cursor or
##    the rowcount of the last operation is not determinable by the interface.[7]
##    NOTE: -- adodbapi returns "-1" by default for all select statements
    rowcount = -1

    # Arraysize specifies the number of rows to fetch at a time with fetchmany().
    arraysize = 1

    def __init__(self, connection):
        self.messages = []
        self.connection = connection
        self.rs = None
        self.description = None
        self.errorhandler = connection.errorhandler

    def __iter__(self):
        return iter(self.fetchone, None)

    def __enter__(self):
        "Allow database cursors to be used with context managers."
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        "Allow database cursors to be used with context managers."
        self.close()

    def __del__(self):
        try:
            self.close()
        except:
        	pass

    def _raiseCursorError(self, errorclass, errorvalue):
        eh = self.errorhandler
        if eh is None:
            eh = standardErrorHandler
        eh(self.connection, self, errorclass, errorvalue)

    def _description_from_recordset(self, recordset):
    	# Abort if closed or no recordset.
        if (recordset is None) or (recordset.State == adStateClosed):
            self.rs = None
            self.description = None
            return

        # Since we use a forward-only cursor, rowcount will always return -1
        self.rowcount = -1
        self.rs = recordset
        desc = list()

        for f in self.rs.Fields:
            display_size = None
            if not(self.rs.EOF or self.rs.BOF):
                display_size = f.ActualSize

            null_ok = bool(f.Attributes & adFldMayBeNull)

            desc.append( (f.Name, f.Type, display_size, f.DefinedSize, f.Precision, f.NumericScale, null_ok) )

        self.description = desc

    def close(self):
        """Close the cursor."""
        self.messages = []
        self.connection = None
        if self.rs and self.rs.State != adStateClosed:
            self.rs.Close()
            self.rs = None

    def _new_command(self, command_type=adCmdText):
        self.cmd = None
        self.messages = []

        if self.connection is None:
            self._raiseCursorError(InterfaceError, None)
            return

        # Inner import to make this module importable on non-Windows platforms.
        import win32com.client

        try:
            self.cmd = win32com.client.Dispatch("ADODB.Command")
            self.cmd.ActiveConnection = self.connection.adoConn
            self.cmd.CommandTimeout = self.connection.adoConn.CommandTimeout
            self.cmd.CommandType = command_type
        except:
            self._raiseCursorError(DatabaseError, None)

    def _execute_command(self):
        # Sprocs may have an integer return value
        self.return_value = None

        try:
            recordset = self.cmd.Execute()
            self.rowcount = recordset[1]
            self._description_from_recordset(recordset[0])
        except Exception as e:
            _message = ""
            if hasattr(e, 'args'): _message += str(e.args)+"\n"
            _message += "Command:\n%s\nParameters:\n%s" %  (self.cmd.CommandText, format_parameters(self.cmd.Parameters, True))
            klass = self.connection._suggest_error_class()
            self._raiseCursorError(klass, _message)


    def callproc(self, procname, parameters=None):
        """Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each
        argument that the sproc expects. The result of the
        call is returned as modified copy of the input
        sequence. Input parameters are left untouched, output and
        input/output parameters replaced with possibly new values.

        The sproc may also provide a result set as output,
        which is available through the standard .fetch*() methods.

        Extension: A "return_value" property may be set on the
        cursor if the sproc defines an integer return value.
        """
        self._new_command(adCmdStoredProc)
        self.cmd.CommandText = procname
        self.cmd.Parameters.Refresh()

        try:
            # Return value is 0th ADO parameter. Skip it.
            for i, p in enumerate(tuple(self.cmd.Parameters)[1:]):
                _configure_parameter(p, parameters[i])
        except:
            _message = 'Converting Parameter %s: %s, %s\n' %\
                (p.Name, ado_type_name(p.Type), repr(parameters[i]))

            self._raiseCursorError(DataError, _message)

        self._execute_command()

        p_return_value = self.cmd.Parameters(0)
        self.return_value = _convert_to_python(p_return_value.Value, p_return_value.Type)

        return [_convert_to_python(p.Value, p.Type)
            for p in tuple(self.cmd.Parameters)[1:] ]


    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command).

        Return value is not defined.
        """
        self._new_command()

        if parameters is None:
            parameters = list()

        parameter_replacements = list()
        for i, value in enumerate(parameters):
            if value is None:
                parameter_replacements.append('NULL')
                continue

            if isinstance(value, six.string_types) and value == "":
                parameter_replacements.append("''")
                continue

            # Otherwise, process the non-NULL, non-empty string parameter.
            parameter_replacements.append('?')
            try:
                p = self.cmd.CreateParameter('p%i' % i, _ado_type(value))
            except KeyError:
                _message = 'Failed to map python type "%s" to an ADO type' % (value.__class__.__name__,)
                self._raiseCursorError(DataError, _message)
            except:
                _message = 'Creating Parameter p%i, %s' % (i, _ado_type(value))
                self._raiseCursorError(DataError, _message)

            try:
                _configure_parameter(p, value)
                self.cmd.Parameters.Append(p)
            except Exception as e:
                _message = 'Converting Parameter %s: %s, %s\n' %\
                    (p.Name, ado_type_name(p.Type), repr(value))

                self._raiseCursorError(DataError, _message)

        # Replace params with ? or NULL
        if parameter_replacements:
            operation = operation % tuple(parameter_replacements)

        self.cmd.CommandText = operation
        self._execute_command()

    def executemany(self, operation, seq_of_parameters):
        """Execute the given command against all parameter sequences or mappings given in seq_of_parameters."""
        self.messages = list()
        total_recordcount = 0

        for params in seq_of_parameters:
            self.execute(operation, params)

            if self.rowcount == -1:
                total_recordcount = -1

            if total_recordcount != -1:
                total_recordcount += self.rowcount

        self.rowcount = total_recordcount

    def _fetch(self, rows=None):
        """Fetch rows from the current recordset.

        rows -- Number of rows to fetch, or None (default) to fetch all rows.
        """
        if self.connection is None or self.rs is None:
            self._raiseCursorError(FetchFailedError, 'Attempting to fetch from a closed connection or empty record set')
            return

        if self.rs.State == adStateClosed or self.rs.BOF or self.rs.EOF:
            if rows == 1: # fetchone returns None
                return None
            else: # fetchall and fetchmany return empty lists
                return list()

        if rows:
            ado_results = self.rs.GetRows(rows)
        else:
            ado_results = self.rs.GetRows()

        py_columns = list()
        column_types = [column_desc[1] for column_desc in self.description]
        for ado_type, column in zip(column_types, ado_results):
            py_columns.append( [_convert_to_python(cell, ado_type) for cell in column] )

        return tuple(zip(*py_columns))

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to executeXXX()
        did not produce any result set or no call was issued yet.
        """
        self.messages = list()
        result = self._fetch(1)
        if result: # return record (not list of records)
            return result[0]
        return None

    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result, returning a list of tuples. An empty sequence is returned when no more rows are available."""
        self.messages = list()
        if size is None:
            size = self.arraysize
        return self._fetch(size)

    def fetchall(self):
        """Fetch all remaining rows of a query result, returning them as a sequence of sequences."""
        self.messages = list()
        return self._fetch()

    def nextset(self):
        """Skip to the next available recordset, discarding any remaining rows from the current recordset.

        If there are no more sets, the method returns None. Otherwise, it returns a true
        value and subsequent calls to the fetch methods will return rows from the next result set.
        """
        self.messages = list()
        if self.connection is None or self.rs is None:
            self._raiseCursorError(Error, None)
            return None

        recordset = self.rs.NextRecordset()[0]
        if recordset is None:
            return None

        self._description_from_recordset(recordset)
        return True

    def setinputsizes(self, sizes): pass
    def setoutputsize(self, size, column=None): pass

# Type specific constructors as required by the DB-API 2 specification.
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
Binary = six.memoryview

def DateFromTicks(ticks):
    """Construct an object holding a date value from the given # of ticks."""
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    """Construct an object holding a time value from the given # of ticks."""
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    """Construct an object holding a timestamp value from the given # of ticks."""
    return Timestamp(*time.localtime(ticks)[:6])

adoIntegerTypes = (adInteger,adSmallInt,adTinyInt,adUnsignedInt,adUnsignedSmallInt,adUnsignedTinyInt,adError)
adoRowIdTypes = (adChapter,)
adoLongTypes = (adBigInt, adUnsignedBigInt, adFileTime)
adoExactNumericTypes = (adDecimal, adNumeric, adVarNumeric, adCurrency)
adoApproximateNumericTypes = (adDouble, adSingle)
adoStringTypes = (adBSTR,adChar,adLongVarChar,adLongVarWChar,adVarChar,adVarWChar,adWChar,adGUID)
adoBinaryTypes = (adBinary, adLongVarBinary, adVarBinary)
adoDateTimeTypes = (adDBTime, adDBTimeStamp, adDate, adDBDate)

# Required DBAPI type specifiers
STRING   = _DbType(adoStringTypes)
BINARY   = _DbType(adoBinaryTypes)
NUMBER   = _DbType((adBoolean,) + adoIntegerTypes + adoLongTypes + adoExactNumericTypes + adoApproximateNumericTypes)
DATETIME = _DbType(adoDateTimeTypes)
# Not very useful for SQL Server, as normal row ids are usually just integers.
ROWID    = _DbType(adoRowIdTypes)


# Mapping ADO data types to Python objects.
def _convert_to_python(variant, adType):
    if variant is None:
        return None
    return _variantConversions[adType](variant)

def _cvtDecimal(variant):
    return _convertNumberWithCulture(variant, decimal.Decimal)

def _cvtFloat(variant):
    return _convertNumberWithCulture(variant, float)

def _convertNumberWithCulture(variant, f):
    try:
        return f(variant)
    except (ValueError,TypeError,decimal.InvalidOperation):
        try:
            europeVsUS = str(variant).replace(",",".")
            return f(europeVsUS)
        except (ValueError,TypeError): pass

def _cvtComDate(comDate):
    if isinstance(comDate, datetime.datetime):
        dt = comDate
    else:
        date_as_float = float(comDate)
        day_count = int(date_as_float)
        fraction_of_day = abs(date_as_float - day_count)

        dt = (datetime.datetime.fromordinal(day_count + _ordinal_1899_12_31) +
            datetime.timedelta(milliseconds=fraction_of_day * _milliseconds_per_day))

    if getattr(settings, 'USE_TZ', False):
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

_variantConversions = MultiMap(
    {
        adoDateTimeTypes : _cvtComDate,
        adoExactNumericTypes: _cvtDecimal,
        adoApproximateNumericTypes: _cvtFloat,
        (adBoolean,): bool,
        adoLongTypes+adoRowIdTypes : int if six.PY3 else long,
        adoIntegerTypes: int,
        adoBinaryTypes: six.memoryview,
    },
    lambda x: x)

# Mapping Python data types to ADO type codes
def _ado_type(data):
    if isinstance(data, six.string_types):
        return adBSTR
    return _map_to_adotype[type(data)]

_map_to_adotype = {
    six.memoryview: adBinary,
    float: adDouble,
    int: adInteger if six.PY2 else adBigInt,
    bool: adBoolean,
    decimal.Decimal: adDecimal,
    datetime.date: adDate,
    datetime.datetime: adDate,
    datetime.time: adDate,
}

if six.PY3:
    _map_to_adotype[bytes] = adBinary

if six.PY2:
    _map_to_adotype[long] = adBigInt
