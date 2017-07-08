import os.path
import subprocess
import itertools

python_versions = ['2.7', '3.3', '3.4', '3.5']
django_versions = ['1.9.13', '1.10.7', '1.11.3']
pytds_versions = ['1.8.2']

# TODO find python
python_exe = 'python2.7'
virtualenv_exe = 'virtualenv'
pip_exe = 'pip'
git_exe = 'git'

build_folder = 'build'


def run_tests(django_ver, pytds_ver):
    root = os.getcwd()
    venv_folder = os.path.join(build_folder, 'venv')

    if not os.path.isdir(venv_folder):
        # using system-site-packages to get pywin32 package which is not installable via pip
        subprocess.check_call([virtualenv_exe, venv_folder, '--system-site-packages'])#, '--python', python_exe])

    venv_pip = os.path.join(venv_folder, 'scripts', 'pip')
    venv_python = os.path.join(venv_folder, 'scripts', 'python')

    # install Django test requirements
    subprocess.check_call([venv_pip, 'install', 'django==' + django_ver])

    subprocess.check_call([venv_pip, 'install', 'django-mssql'])

    # install pytds
    subprocess.check_call([venv_pip, 'install', 'python-tds=={}'.format(pytds_ver)])

    # install django-mssql
    #subprocess.check_call([venv_pip, 'install', 'git+https://bitbucket.org/Manfre/django-mssql.git#egg=django-mssql'])

    runtests_path = os.path.join('tests', 'runtests.py')
    env = os.environ.copy()
    env['PYTHONPATH'] = ':'.join(['tests', '.'])
    params = [venv_python, runtests_path, '--noinput', '--settings=test_mssql']
    subprocess.check_call(params, env=env)


if __name__ == '__main__':
    for django_ver in django_versions:
        for pytds_ver in pytds_versions:
            run_tests(django_ver, pytds_ver)
    print('PASS')
