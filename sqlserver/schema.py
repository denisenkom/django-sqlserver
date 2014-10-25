from __future__ import absolute_import, unicode_literals
import binascii
import django.db.models.fields
from django.utils import six
import sqlserver_ado.schema


class DatabaseSchemaEditor(sqlserver_ado.schema.DatabaseSchemaEditor):
    def effective_default(self, field):
        """
        Returns a field's effective database default value
        """
        if field.has_default():
            default = field.get_default()
        elif not field.null and field.blank and field.empty_strings_allowed:
            if isinstance(field, django.db.models.fields.BinaryField):
                default = b''
            else:
                default = ""
        else:
            default = None
        # If it's a callable, call it
        if callable(default):
            default = default()
        return default

    def quote_value(self, value):
        if isinstance(value, six.text_type):
            return "'%s'" % six.text_type(value).replace("\'", "\'\'")
        elif isinstance(value, bytes):
            return "0x" + binascii.hexlify(value)
        else:
            return super(DatabaseSchemaEditor, self).quote_value(value)
