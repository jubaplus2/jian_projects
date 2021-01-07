# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright (c) 2016-2017 Anaconda, Inc.
#
# May be copied and distributed freely only as part of an Anaconda or
# Miniconda installation.
# -----------------------------------------------------------------------------
"""Launch applications utilities."""

# yapf: disable

# Standard library imports
import codecs
import glob
import json
import os
import shutil
import subprocess
import sys
import uuid

# Local imports
from anaconda_navigator.api.anaconda_api import AnacondaAPI
from anaconda_navigator.config import (CONF_PATH, HOME_PATH,
                                       LAUNCH_SCRIPTS_PATH, LINUX, MAC, WIN)
from anaconda_navigator.utils.logs import logger


# yapf: enable

if WIN:
    import ctypes


def get_scripts_path(root_prefix, prefix):
    """Return the launch scripts path."""
    scripts_path = LAUNCH_SCRIPTS_PATH
    if root_prefix != prefix:
        scripts_path = os.path.join(scripts_path, prefix.split('/')[-1])
    return scripts_path


def get_quotes(prefix):
    """Return quotes if needed for spaces on prefix."""
    return '"' if ' ' in prefix and '"' not in prefix else ''


def remove_package_logs(root_prefix, prefix):
    """Try to remove output, error logs for launched applications."""
    scripts_p = get_scripts_path(root_prefix, prefix)
    if not os.path.isdir(scripts_p):
        return

    scripts_p = scripts_p if scripts_p[-1] == os.sep else scripts_p + os.sep
    files = glob.glob(scripts_p + '*.txt')
    for file_ in files:
        log_path = os.path.join(scripts_p, file_)
        try:
            os.remove(log_path)
        except Exception:
            pass


def get_package_logs(package_name, prefix=None, root_prefix=None, id_=None):
    """Return the package log names for launched applications."""
    scripts_path = get_scripts_path(root_prefix, prefix)
    if os.path.isdir(scripts_path):
        files = os.listdir(scripts_path)
    else:
        files = []

    if id_ is None:
        for i in range(1, 10000):
            stdout_log = "{package_name}-out-{i}.txt".format(
                package_name=package_name, i=i
            )
            stderr_log = "{package_name}-err-{i}.txt".format(
                package_name=package_name, i=i
            )
            if stdout_log not in files and stderr_log not in files:
                id_ = i
                break
    else:
        stdout_log = "{package_name}-out-{i}.txt".format(
            package_name=package_name, i=id_
        )
        stderr_log = "{package_name}-err-{i}.txt".format(
            package_name=package_name, i=id_
        )

    if prefix and root_prefix:
        stdout_log_path = os.path.join(scripts_path, stdout_log)
        stderr_log_path = os.path.join(scripts_path, stderr_log)
    else:
        stdout_log_path = stdout_log
        stderr_log_path = stderr_log

    return stdout_log_path, stderr_log_path, id_


def is_program_installed(basename):
    """
    Return program absolute path if installed in PATH.

    Otherwise, return None
    """
    for path in os.environ["PATH"].split(os.pathsep):
        abspath = os.path.join(path, basename)
        if os.path.isfile(abspath):
            return abspath


def run_app_on_win(
    prefix, command, package_name, root_prefix, environment=None
):
    """Run application on windows system and enforce proper env activation."""
    stdout_log_path, stderr_log_path, id_ = get_package_logs(
        package_name,
        root_prefix=root_prefix,
        prefix=prefix,
    )
    quote = get_quotes(prefix)
    codepage = str(ctypes.cdll.kernel32.GetACP())
    # Call is needed to avoid the batch script from closing after running
    # the first (environment activation) line
    prefix = prefix.replace('\\', '/')
    cmd = (
        'chcp {CODEPAGE}\n'
        'call {QUOTE}{CONDA_PREFIX}/Scripts/activate{QUOTE} '
        '{QUOTE}{CONDA_PREFIX}{QUOTE}\n'
        '{QUOTE}{COMMAND}{QUOTE} '
        '>{QUOTE}{OUT}{QUOTE} 2>{QUOTE}{ERR}{QUOTE}\n'
    ).format(
        CODEPAGE=codepage,
        CONDA_PREFIX=prefix,
        COMMAND=command,
        QUOTE=quote,
        OUT=stdout_log_path,
        ERR=stderr_log_path,
    )
    cmd = cmd.replace('/', '\\')  # Turn slashes back to windows standard

    suffix = 'bat'
    fpath = create_app_run_script(
        cmd,
        package_name,
        prefix,
        root_prefix,
        suffix,
    )
    CREATE_NO_WINDOW = 0x08000000
    pid = subprocess.Popen(
        fpath,
        creationflags=CREATE_NO_WINDOW,
        shell=True,
        cwd=HOME_PATH,
        env=environment,
    ).pid
    return pid, id_


def run_app_on_unix(
    prefix, command, package_name, root_prefix, environment=None
):
    """Run application on unix system and enforce proper env activation."""
    if command.startswith('open'):
        command = command.replace('open ', '')
        start = 'open '
    else:
        start = ''

    stdout_log_path, stderr_log_path, id_ = get_package_logs(
        package_name,
        root_prefix=root_prefix,
        prefix=prefix,
    )
    quote = get_quotes(prefix)
    prefix = prefix.replace('\\', '/')
    cmd = (
        '#!/usr/bin/env bash\n'
        'source {QUOTE}{CONDA_PREFIX}/bin/activate{QUOTE} '
        '{QUOTE}{CONDA_PREFIX}{QUOTE}\n'
        '{START}{QUOTE}{COMMAND}{QUOTE} '
        '>{QUOTE}{OUT}{QUOTE} 2>{QUOTE}{ERR}{QUOTE}\n'
    ).format(
        CONDA_PREFIX=prefix,
        COMMAND=command,
        START=start,
        QUOTE=quote,
        OUT=stdout_log_path,
        ERR=stderr_log_path,
    )
    suffix = 'sh'
    fpath = create_app_run_script(
        cmd,
        package_name,
        prefix,
        root_prefix,
        suffix,
    )
    pid = subprocess.Popen(
        fpath,
        shell=False,
        cwd=HOME_PATH,
        env=environment,
    ).pid
    return pid, id_


def create_app_run_script(command, package_name, prefix, root_prefix, suffix):
    """Create the script to run the application and activate th eenvironemt."""
    # qtpy is adding this to env on startup and this is messing qtconsole
    # and other apps on other envs with different versions of QT
    if 'QT_API' in os.environ:
        os.environ.pop('QT_API')

    package_name = package_name or 'app'

    scripts_path = get_scripts_path(root_prefix, prefix)
    if not os.path.isdir(scripts_path):
        os.makedirs(scripts_path)
    fpath = os.path.join(scripts_path, '{0}.{1}'.format(package_name, suffix))

    # Try to clean log files
    remove_package_logs(root_prefix=root_prefix, prefix=prefix)

    # Create the launch script
    if WIN:
        codepage = str(ctypes.cdll.kernel32.GetACP())
        cp = 'cp' + codepage
        with codecs.open(fpath, "w", cp) as f:
            f.write(command)
    else:
        # Unicode is disabled on unix systems until properly fixed!
        # Using normal open and not codecs.open
        # cp = 'utf-8'
        with open(fpath, "w") as f:
            f.write(command)

    os.chmod(fpath, 0o777)

    return fpath


def launch(
    prefix,
    command,
    leave_path_alone,
    working_directory=HOME_PATH,
    package_name=None,
    root_prefix=None,
    environment=None,
):
    """Handle launching commands from projects."""
    logger.debug(str((prefix, command)))
    command = command.replace('\\', '/')
    prefix = prefix.replace('\\', '/')
    root_prefix = root_prefix.replace('\\', '/')

    pid = -1

    if os.name == 'nt' and not leave_path_alone:
        command = command.replace('/bin', '/Scripts')

    if MAC or LINUX:
        pid = run_app_on_unix(
            prefix=prefix,
            command=command,
            package_name=package_name,
            root_prefix=root_prefix,
            environment=environment,
        )
    else:
        pid = run_app_on_win(
            prefix=prefix,
            command=command,
            package_name=package_name,
            root_prefix=root_prefix,
            environment=environment,
        )
    return pid


def console(activate=None, working_directory=HOME_PATH):
    """Open command prompt console and optionally activate the environment."""
    cwd = working_directory

    if os.name == 'nt':
        if activate:
            cmd = 'start cmd.exe /k activate ' + activate
        else:
            cmd = 'start cmd.exe'
        logger.debug(cmd)
        subprocess.Popen(cmd, shell=True, cwd=cwd)
    elif sys.platform == 'darwin':
        if activate:
            cmd = (
                'bash --init-file '
                '<(echo "source activate {};")'.format(activate)
            )
        else:
            cmd = 'bash'
        fname = os.path.join(CONF_PATH, 'a.tool')

        with open(fname, 'w') as f:
            f.write(cmd)
        os.chmod(fname, 0o777)

        logger.debug('open ' + fname)
        subprocess.call(['open ' + fname], shell=True, cwd=cwd)
    else:  # Linux, solaris, etc
        if is_program_installed('gnome-terminal'):
            if activate:
                cmd = [
                    'gnome-terminal',
                    '-x',
                    'bash',
                    '-c',
                    'bash --init-file'
                    ' <(echo "source activate {0};")'.format(activate),
                ]
            else:
                cmd = ['gnome-terminal', '-e', 'bash']
            logger.debug(' '.join(cmd))
            subprocess.Popen(cmd, cwd=cwd)
        elif is_program_installed('xterm'):
            if activate:
                cmd = [
                    'xterm',
                    '-e',
                    'bash --init-file'
                    ' <(echo "source activate {0};")'.format(activate),
                ]
            else:
                cmd = ['xterm']
            logger.debug(' '.join(cmd))
            subprocess.Popen(cmd, cwd=cwd)


def check_prog(prog, prefix=None):
    """Check if program exists in prefix."""
    api = AnacondaAPI()
    prefix = prefix or api.conda_get_prefix_envname(name='root')

    if prog in ['notebook', 'jupyter notebook']:
        pkgs = ['notebook', 'ipython-notebook', 'jupyter-notebook']
    elif prog in ['ipython', 'jupyter console']:
        pkgs = ['ipython', 'jupyter']
    else:
        pkgs = [prog]

    return any(
        api.conda_package_version(
            prefix=prefix, pkg=p
        ) is not None for p in pkgs
    )


def py_in_console(activate=None, prog='python'):
    """
    Run (i)python in a new console.

    It optionally run activate first on the given env name/path.
    """
    logger.debug("%s, %s", activate, prog)
    if not check_prog(prog, activate):
        raise RuntimeError(
            'Program not available in environment: %s, %s', prog, activate
        )
    if prog == 'python':
        cmd = 'python -i'
    elif prog == 'ipython':
        cmd = 'ipython -i'
    elif 'notebook' in prog:
        cmd = 'jupyter notebook'
    if os.name == 'nt':
        if activate:
            cmd = '"activate {} & {}"'.format(activate, cmd)
        else:
            cmd = '"{}"'.format(cmd)
        logger.debug('start cmd.exe /k ' + cmd)
        subprocess.Popen(
            'start cmd.exe /k {}'.format(cmd),
            shell=True,
            cwd=HOME_PATH,
        )
    elif sys.platform == 'darwin':
        if activate:
            if '/' not in activate:
                out = subprocess.check_output(['conda', 'env', 'list'])
                for line in out.decode().split('\n'):
                    # This is too fragile...!
                    if line and line.split()[0] == activate:
                        activate = line.split()[-1]
                        break
            cmd = activate + '/bin/' + cmd
        else:
            cmd = cmd
        fname = os.path.join(CONF_PATH, 'a.tool')

        command = "source activate {0};{1}".format(activate, cmd)
        with open(fname, 'w') as f:
            f.write(command)

        os.chmod(fname, 0o777)
        logger.debug('open ' + fname)
        subprocess.call(['open ' + fname], shell=True, cwd=HOME_PATH)
    else:  # linux, solaris, etc
        if activate:
            cmd = "source activate " + activate + "; " + cmd
        if is_program_installed('gnome-terminal'):
            cmd = ['gnome-terminal', '-x', 'bash', '-c', cmd]
            logger.debug(' '.join(cmd))
            subprocess.Popen(cmd, cwd=HOME_PATH)
            return
        elif is_program_installed('xterm'):
            cmd = ['xterm', '-e', cmd]
            logger.debug(' '.join(cmd))
            subprocess.Popen(cmd, cwd=HOME_PATH)
            return


def run_ex(*args):
    """Start new console window, run command (given as string list)."""
    logger.debug(str(args))
    cmd = '{0}'.format(" ".join(args))

    if os.name == 'nt':
        subprocess.Popen(['start', 'cmd.exe', '/K', cmd])

    elif sys.platform == 'darwin':
        fname = 'a.tool'
        open(fname, 'w').write(cmd)
        os.chmod(fname, 0o777)
        subprocess.call(['open ' + fname], shell=True)
        os.unlink(fname)

    else:
        terms = ['gnome-terminal', 'konsole', 'xterm']
        for term in terms:
            if is_program_installed(term):
                subprocess.Popen([term, '-e', '"{}"'.format(cmd)])
                break


def run_notebook(project_path, project=None, filename=""):
    """Start notebook server."""
    from anaconda_navigator.api import AnacondaAPI
    api = AnacondaAPI()

    if project is None:
        project = api.load_project(project_path)

    kernels_folder = os.sep.join([HOME_PATH, ".ipython", "kernels"])
    display_name = '{0} ({1})'.format(project.name, project_path)
    kernel_uuid = uuid.uuid1()
    kernel_path = os.sep.join([kernels_folder, "{name}", "kernel.json"])
    pyexec = os.sep.join([project.env_prefix(project_path), 'bin', 'python'])
    spec = {
        "argv": [pyexec, "-m", "IPython.kernel", "-f", "{connection_file}"],
        "display_name": display_name,
        "language": "python",
    }

    # Delete any other kernel sec mathching this name!
    kernels = os.listdir(kernels_folder)
    for kernel in kernels:
        path = os.sep.join([kernels_folder, kernel])
        file_path = os.sep.join([path, 'kernel.json'])

        if os.path.isfile(file_path):
            with open(file_path, 'r') as f:
                data = json.loads(f.read())

            name = data.get('display_name')
            if name is not None and project_path in name:
                shutil.rmtree(path)

    os.makedirs(os.path.split(kernel_path.format(name=kernel_uuid))[0])

    with open(kernel_path.format(name=kernel_uuid), 'w') as f:
        f.write(json.dumps(spec))

    # This is not working!
    cmd = (
        'jupyter notebook '
        '--KernelSpecManager.whitelist={0}'.format(kernel_uuid)
    )

    cmd = ('jupyter notebook')
    command = (cmd + ' ' + filename)
    logger.debug(",".join([command, project_path]))
    subprocess.Popen(command.split(), cwd=project_path)


def run_python_file(project_path, project=None, filename=""):
    """Execute python in environment."""
    from anaconda_navigator.api import AnacondaAPI
    api = AnacondaAPI()

    if project is None:
        project = api.load_project(project_path)

    cmd = os.sep.join([project.env_prefix(project_path), 'bin', 'python'])
    logger.debug(",".join([cmd, filename]))
    subprocess.Popen([cmd, filename])


def run_python_console(project_path, project=None, filename=""):
    """Execute python in env with console."""
    from anaconda_navigator.api import AnacondaAPI
    api = AnacondaAPI()

    if project is None:
        project = api.load_project(project_path)

    cmd = os.sep.join([project.env_prefix(project_path), 'bin', 'python'])
    run_ex(cmd, filename)