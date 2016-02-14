# This is an example test settings file for use with the Django test suite.
#
# The 'sqlite3' backend requires only the ENGINE setting (an in-
# memory database will be used). All other backends will require a
# NAME and potentially authentication information. See the
# following section in the docs for more information:
#
# https://docs.djangoproject.com/en/dev/internals/contributing/writing-code/unit-tests/
#
# The different databases that Django supports behave differently in certain
# situations, so it is recommended to run the test suite against as many
# database backends as possible.  You may want to create a separate settings
# file for each of the backends you test against.
import os

INSTANCE = os.environ.get('SQLINSTANCE', '')
HOST = os.environ.get('COMPUTERNAME', os.environ.get('HOST', 'localhost'))
if INSTANCE:
    HOST = '\\'.join([HOST, INSTANCE])
DATABASE = os.environ.get('DATABASE_NAME', 'django_test_backend')
USER = os.environ.get('SQLUSER', 'sa')
PASSWORD = os.environ.get('SQLPASSWORD', 'sa')

DATABASES = {
    'default': {
        'ENGINE': os.environ.get('BACKEND', 'sqlserver'),
        'NAME': DATABASE,
        'TEST_NAME': DATABASE,
        'HOST': HOST,
        'USER': USER,
        'PASSWORD': PASSWORD,
        'OPTIONS': {
            'provider': os.environ.get('ADO_PROVIDER', 'SQLNCLI11'),
            # 'extra_params': 'DataTypeCompatibility=80;MARS Connection=True;',
            'use_legacy_date_fields': False,
        },
    },
    'other': {
        'ENGINE': os.environ.get('BACKEND', 'sqlserver'),
        'NAME': DATABASE + '_other',
        'TEST_NAME': DATABASE + '_other',
        'HOST': HOST,
        'USER': USER,
        'PASSWORD': PASSWORD,
        'OPTIONS': {
            'provider': os.environ.get('ADO_PROVIDER', 'SQLNCLI11'),
            # 'extra_params': 'DataTypeCompatibility=80;MARS Connection=True;',
            'use_legacy_date_fields': False,
        },
    }
}

MIDDLEWARE_CLASSES = [
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.auth.middleware.SessionAuthenticationMiddleware',
]

SECRET_KEY = "django_tests_secret_key"
# To speed up tests under SQLite we use the MD5 hasher as the default one. 
# This should not be needed under other databases, as the relative speedup
# is only marginal there.
PASSWORD_HASHERS = (
    'django.contrib.auth.hashers.MD5PasswordHasher',
)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': 'ERROR',
        },
    },
}
