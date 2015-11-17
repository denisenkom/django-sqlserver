import os
import re
from setuptools import setup


def find_version(*file_paths):
    with open(os.path.join(os.path.dirname(__file__), *file_paths)) as handle:
        version_file = handle.read()
    version_match = re.search("^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name="django-sqlserver",
    version=find_version("sqlserver", "__init__.py"),
    url='https://github.com/denisenkom/django-sqlserver',
    license='MIT',
    description="Django backend database support for MS SQL Server and pytds.",
    author='Mikhail Denisenko',
    author_email='denisenkom@gmail.com',
    packages=['sqlserver'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Framework :: Django',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Database',
    ],
    install_requires=['django-mssql>=1.7'],
    zip_safe=True,
)
