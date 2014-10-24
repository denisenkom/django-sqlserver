from __future__ import unicode_literals
import sys

if sys.version_info >= (2, 7):
    # Django Ticket #17671 - Allow using a cursor as a ContextManager
    # in Python 2.7
    from django.db.backends.util import CursorWrapper
    if not hasattr(CursorWrapper, '__enter__'):
        enter = lambda self: self
        exit = lambda self, type, value, traceback: self.cursor.__exit__(type, value, traceback)

        CursorWrapper.__enter__ = enter
        CursorWrapper.__exit__ = exit
