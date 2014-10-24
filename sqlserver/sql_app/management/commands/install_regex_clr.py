from __future__ import unicode_literals
from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Installs the regex_clr.dll assembly with the database"

    requires_model_validation = False

    args = 'database_name'

    def handle(self, database_name=None, *args, **options):
        if not database_name:
            self.print_help('manage.py', 'install_regex_clr')
            return

        connection.creation.install_regex_clr(database_name)
        print('Installed regex_clr to database %s' % database_name)
