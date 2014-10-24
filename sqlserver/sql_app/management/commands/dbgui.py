from __future__ import unicode_literals
from django.core.management.base import NoArgsCommand

class Command(NoArgsCommand):
    help = "Launches SQL Server Management Studio (on Windows)."

    requires_model_validation = False

    def handle_noargs(self, **options):
        from django.conf import settings
        import os

        args = ['-nosplash', '-E']
        
        host = settings.DATABASE_OPTIONS.get('host', settings.DATABASE_HOST)
        db = settings.DATABASE_OPTIONS.get('db', settings.DATABASE_NAME)
        # user = settings.DATABASE_OPTIONS.get('user', settings.DATABASE_USER)
        # passwd = settings.DATABASE_OPTIONS.get('passwd', settings.DATABASE_PASSWORD)
        # port = settings.DATABASE_OPTIONS.get('port', settings.DATABASE_PORT)
    
        if host:
            args += ['-S', host]

        if db:
            args += ["-d", db]

        os.execvp('sqlwb.exe', args)
