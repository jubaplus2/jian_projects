# coding: utf-8
"""JupyterLab entry points"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import print_function
from distutils.version import LooseVersion
import errno
import json
import logging
import pipes
import os
import glob
from functools import partial
from os import path as osp
from os.path import join as pjoin
from tornado import gen
from tornado.ioloop import IOLoop
from subprocess import CalledProcessError, Popen, STDOUT
import shutil
import sys
import tarfile
from jupyter_core.paths import ENV_JUPYTER_PATH, jupyter_config_path
from notebook.nbextensions import (
    GREEN_ENABLED, GREEN_OK, RED_DISABLED, RED_X
)

from .semver import Range, gte, lt, lte, gt
from ._version import __version__


if sys.platform == 'win32':
    from subprocess import list2cmdline
else:
    def list2cmdline(cmd_list):
        return ' '.join(map(pipes.quote, cmd_list))


here = osp.dirname(osp.abspath(__file__))
logging.basicConfig(format='%(message)s', level=logging.INFO)


def get_app_dir(app_dir=None):
    """Get the configured JupyterLab app directory.
    """
    app_dir = app_dir or os.environ.get('JUPYTERLAB_DIR')
    app_dir = app_dir or pjoin(ENV_JUPYTER_PATH[0], 'lab')
    return os.path.realpath(app_dir)


def get_user_settings_dir():
    """Get the configured JupyterLab app directory.
    """
    settings_dir = os.environ.get('JUPYTERLAB_SETTINGS_DIR')
    settings_dir = settings_dir or pjoin(
        jupyter_config_path()[0], 'lab', 'user-settings'
    )
    return os.path.realpath(settings_dir)


@gen.coroutine
def run(cmd, **kwargs):
    """Run a command in the given working directory.
    """
    logger = kwargs.pop('logger', logging) or logging
    abort_callback = kwargs.pop('abort_callback', None)
    logger.info('> ' + list2cmdline(cmd))
    kwargs.setdefault('shell', sys.platform == 'win32')
    kwargs.setdefault('env', os.environ)
    kwargs.setdefault('stderr', STDOUT)
    yield gen.moment  # Sync up to the iterator
    try:
        proc = Popen(cmd, **kwargs)
        # Poll the process once per second until finished.
        while 1:
            yield gen.sleep(1)
            if proc.poll() is not None:
                break
            if abort_callback and abort_callback():
                raise ValueError('Aborted')
    except CalledProcessError as error:
        output = error.output.decode('utf-8')
        logger.info(output)
        raise error
    finally:
        proc.wait()


def install_extension(extension, app_dir=None, logger=None):
    """Install an extension package into JupyterLab.

    Follows the semantics of https://docs.npmjs.com/cli/install.

    The extension is first validated.

    If link is true, the source directory is linked using `npm link`.
    """
    func = partial(install_extension_async, extension, app_dir=app_dir,
                   logger=logger)
    return IOLoop.instance().run_sync(func)


@gen.coroutine
def install_extension_async(extension, app_dir=None, logger=None, abort_callback=None):
    """Install an extension package into JupyterLab.

    Follows the semantics of https://docs.npmjs.com/cli/install.

    The extension is first validated.

    If link is true, the source directory is linked using `npm link`.
    """
    app_dir = get_app_dir(app_dir)
    logger = logger or logging
    if app_dir == here:
        raise ValueError('Cannot install extensions in core app')
    extension = _normalize_path(extension)

    # Check for a core extensions here.
    data = _get_core_data()
    if extension in _get_core_extensions():
        config = _get_build_config(app_dir)
        uninstalled = config.get('uninstalled_core_extensions', [])
        if extension in uninstalled:
            uninstalled.remove(extension)
            config['uninstalled_core_extensions'] = uninstalled
            _write_build_config(config, app_dir, logger=logger)
        return

    _ensure_app_dirs(app_dir, logger)
    
    target = pjoin(app_dir, 'extensions', 'temp')
    if os.path.exists(target):
        shutil.rmtree(target)
    os.makedirs(target)

    # npm pack the extension
    yield run([get_npm_name(), 'pack', extension], cwd=target, logger=logger, abort_callback=abort_callback)

    fname = os.path.basename(glob.glob(pjoin(target, '*.*'))[0])
    data = _read_package(pjoin(target, fname))

    # Remove the tarball if the package is not an extension.
    if not _is_extension(data):
        shutil.rmtree(target)
        msg = '%s is not a valid JupyterLab extension' % extension
        raise ValueError(msg)

    # Remove the tarball if the package is not compatible.
    core_data = _get_core_data()
    deps = data.get('dependencies', dict())
    errors = _validate_compatibility(extension, deps, core_data)
    if errors:
        shutil.rmtree(target)
        msg = _format_compatibility_errors(
            data['name'], data['version'], errors
        )
        raise ValueError(msg)

    # Check for existing app extension of the same name.
    extensions = _get_extensions(app_dir)
    if data['name'] in extensions:
        other = extensions[data['name']]
        path = other['path']
        if osp.exists(path) and other['location'] == 'app':
            os.remove(path)

    # Handle any schemas.
    schemaDir = data['jupyterlab'].get('schemaDir', None)
    if schemaDir:
        dest = pjoin(app_dir, 'schemas')
        _copy_tar_files(pjoin(target, fname), schemaDir, dest)

    # Handle a theme.
    themeDir = data['jupyterlab'].get('themeDir', None)
    if themeDir:
        normedName = data['name'].replace('@', '').replace('/', '')
        dest = pjoin(app_dir, 'themes', normedName)
        _copy_tar_files(pjoin(target, fname), themeDir, dest)

    # Remove an existing extension tarball.
    ext_path = pjoin(app_dir, 'extensions', fname)
    if os.path.exists(ext_path):
        os.remove(ext_path)

    shutil.move(pjoin(target, fname), pjoin(app_dir, 'extensions'))
    shutil.rmtree(target)

    # Remove any existing package from staging/node_modules
    target = pjoin(app_dir, 'staging', 'node_modules', data['name'])
    target = target.replace('/', os.sep)
    if os.path.exists(target):
        shutil.rmtree(target)


def link_package(path, app_dir=None, logger=None):
    """Link a package against the JupyterLab build."""
    func = partial(link_package_async, path, app_dir=app_dir, logger=logger)
    return IOLoop.instance().run_sync(func)


@gen.coroutine
def link_package_async(path, app_dir=None, logger=None, abort_callback=None):
    """Link a package against the JupyterLab build.
    """
    logger = logger or logging
    app_dir = get_app_dir(app_dir)
    if app_dir == here:
        raise ValueError('Cannot link packages in core app')

    path = _normalize_path(path)
    _ensure_app_dirs(app_dir, logger)

    # Verify the package.json data.
    pkg_path = osp.join(path, 'package.json')
    if not osp.exists(pkg_path):
        msg = 'Linked package must point to a directory with package.json'
        raise ValueError(msg)

    with open(pkg_path) as fid:
        data = json.load(fid)

    # Check for a core extensions here.
    core_extensions = _get_core_extensions()
    if data['name'] in core_extensions:
        raise ValueError('Cannot link a core extension')

    is_extension = _is_extension(data)
    if is_extension:
        yield install_extension_async(path, app_dir, abort_callback=abort_callback)
    else:
        msg = ('*** Note: Linking non-extension package "%s" (lacks ' +
               '`jupyterlab.extension` metadata)')
        logger.info(msg % data['name'])

    core_data = _get_core_data()
    deps = data.get('dependencies', dict())
    name = data['name']
    errors = _validate_compatibility(name, deps, core_data)
    if errors:
        msg = _format_compatibility_errors(name, data['version'], errors)
        raise ValueError(msg)

    config = _get_build_config(app_dir)
    config.setdefault('linked_packages', dict())
    config['linked_packages'][data['name']] = path
    _write_build_config(config, app_dir, logger=logger)


def unlink_package(package, app_dir=None, logger=None):
    """Unlink a package from JupyterLab by path or name.
    """
    logger = logger or logging
    package = _normalize_path(package)
    name = None
    app_dir = get_app_dir(app_dir)
    if app_dir == here:
        raise ValueError('Cannot link packages in core app')

    config = _get_build_config(app_dir)
    linked = config.setdefault('linked_packages', dict())
    for (key, value) in linked.items():
        if value == package or key == package:
            name = key
            break

    if not name:
        logger.warn('No package matching "%s" is linked' % package)
        return False

    del linked[name]
    config['linked_packages'] = linked
    _write_build_config(config, app_dir, logger=logger)

    extensions = _get_extensions(app_dir)
    if name in extensions:
        uninstall_extension(name, app_dir)

    return True


def enable_extension(extension, app_dir=None, logger=None):
    """Enable a JupyterLab extension.
    """
    _toggle_extension(extension, False, app_dir, logger)


def disable_extension(extension, app_dir=None, logger=None):
    """Disable a JupyterLab package.
    """
    _toggle_extension(extension, True, app_dir, logger)


def get_npm_name():
    """Get the appropriate npm executable name.
    """
    return 'npm.cmd' if os.name == 'nt' else 'npm'


@gen.coroutine
def check_node():
    """Check for the existence of node and whether it is the right version.
    """
    try:
        yield run(['node', 'node-version-check.js'], cwd=here)
    except Exception as e:
        raise ValueError('`node` version 5+ is required, see extensions in README')


def should_build(app_dir=None, logger=None):
    """Determine whether JupyterLab should be built.

    Note: Linked packages should be updated by manually building.

    Returns a tuple of whether a build is necessary, and an associated message.
    """
    logger = logger or logging
    app_dir = get_app_dir(app_dir)

    # Check for installed extensions
    extensions = _get_extensions(app_dir)

    # No linked and no extensions and no built version.
    if not extensions and not os.path.exists(pjoin(app_dir, 'static')):
        return False, ''

    pkg_path = pjoin(app_dir, 'static', 'package.json')
    if not os.path.exists(pkg_path):
        return True, 'Installed extensions with no built application'

    with open(pkg_path) as fid:
        static_data = json.load(fid)

    # Look for mismatched version.
    version = static_data['jupyterlab'].get('version', '')
    if LooseVersion(version) != LooseVersion(__version__):
        msg = 'Version mismatch: %s (built), %s (current)'
        return True, msg % (version, __version__)

    # Look for mismatched extensions.
    template_data = _get_package_template(app_dir, logger)

    template_exts = template_data['jupyterlab']['extensions']

    if set(template_exts) != set(static_data['jupyterlab']['extensions']):
        return True, 'Installed extensions changed'

    template_mime_exts = set(template_data['jupyterlab']['mimeExtensions'])
    staging_mime_exts = set(static_data['jupyterlab']['mimeExtensions'])

    if template_mime_exts != staging_mime_exts:
        return True, 'Installed extensions changed'

    deps = static_data.get('dependencies', dict())

    # Look for mismatched extension paths.
    for name in extensions:
        # Check for dependencies that were rejected as incompatible.
        if name not in template_data['dependencies']:
            continue

        path = deps[name]
        if path.startswith('file:'):
            path = path.replace('file:', '')
            path = os.path.abspath(pjoin(app_dir, 'staging', path))

        template_path = template_data['dependencies'][name]
        if sys.platform == 'win32':
            path = path.lower()
            template_path = template_path.lower()

        if path != template_path:
            return True, 'Installed extensions changed'

    return False, ''


def validate_compatibility(extension, app_dir=None, logger=None):
    """Validate the compatibility of an extension.
    """
    app_dir = get_app_dir(app_dir)
    extensions = _get_extensions(app_dir)

    if extension not in extensions:
        raise ValueError('%s is not an installed extension')

    deps = extensions[extension].get('dependencies', dict())
    core_data = _get_core_data()
    return _validate_compatibility(extension, deps, core_data)


def uninstall_extension(name, app_dir=None, logger=None):
    """Uninstall an extension by name.
    """
    logger = logger or logging
    app_dir = get_app_dir(app_dir)
    if app_dir == here:
        raise ValueError('Cannot install packages in core app')
    # Allow for uninstalled core extensions here.
    data = _get_core_data()
    if name in _get_core_extensions():
        logger.info('Uninstalling core extension %s' % name)
        config = _get_build_config(app_dir)
        uninstalled = config.get('uninstalled_core_extensions', [])
        if name not in uninstalled:
            uninstalled.append(name)
            config['uninstalled_core_extensions'] = uninstalled
            _write_build_config(config, app_dir, logger=logger)
        return True

    for (extname, data) in _get_extensions(app_dir).items():
        path = data['path']
        if extname == name:
            msg = 'Uninstalling %s from %s' % (name, os.path.dirname(path))
            logger.info(msg)
            os.remove(path)
            return True

    logger.warn('No labextension named "%s" installed' % name)
    return False


def list_extensions(app_dir=None, logger=None):
    """List the extensions.
    """
    logger = logger or logging
    app_dir = get_app_dir(app_dir)
    extensions = _get_extensions(app_dir)
    disabled = _get_disabled(app_dir)
    all_linked = _get_linked_packages(app_dir, logger=logger)
    app = []
    sys = []
    linked = []
    errors = dict()

    core_data = _get_core_data()

    # We want to organize by dir.
    sys_path = pjoin(get_app_dir(), 'extensions')
    for (key, value) in extensions.items():
        deps = extensions[key].get('dependencies', dict())
        errors[key] = _validate_compatibility(key, deps, core_data)
        if key in all_linked:
            linked.append(key)
        if value['path'] == sys_path and sys_path != app_dir:
            sys.append(key)
            continue
        app.append(key)

    logger.info('JupyterLab v%s' % __version__)
    logger.info('Known labextensions:')
    if app:
        logger.info('   app dir: %s' % app_dir)
        for item in sorted(app):
            version = extensions[item]['version']
            extra = ''
            if item in disabled:
                extra += ' %s' % RED_DISABLED
            else:
                extra += ' %s' % GREEN_ENABLED
            if errors[item]:
                extra += ' %s' % RED_X
            else:
                extra += ' %s' % GREEN_OK
            logger.info('        %s@%s%s' % (item, version, extra))
            if errors[item]:
                msg = _format_compatibility_errors(item, version, errors[item])
                logger.warn(msg + '\n')

    if sys:
        logger.info('   sys dir: %s' % sys_path)
        for item in sorted(sys):
            version = extensions[item]['version']
            extra = ''
            if item in disabled:
                extra += ' %s' % RED_DISABLED
            else:
                extra += ' %s' % GREEN_ENABLED
            logger.info('        %s%s' % (item, extra))
            if errors[item]:
                extra += ' %s' % RED_X
            else:
                extra += ' %s' % GREEN_OK
            if item in linked:
                extra += '*'
            logger.info('        %s@%s%s' % (item, version, extra))
            if errors[item]:
                msg = _format_compatibility_errors(item, version, errors[item])
                logger.warn(msg + '\n')

    if linked:
        logger.info('   linked extensions:')
        for item in sorted(linked):
            logger.info('        %s: %s' % (item, all_linked[item]))

    if len(all_linked) > len(linked):
        logger.info('   linked packages:')
        for key in sorted(all_linked.keys()):
            if (key in linked):
                continue
            logger.info('        %s: %s' % (key, all_linked[key]))

    # Handle uninstalled and disabled core packages
    uninstalled_core = _get_uinstalled_core_extensions(app_dir)
    if uninstalled_core:
        logger.info('\nUninstalled core extensions:')
        [logger.info('    %s' % item) for item in sorted(uninstalled_core)]

    core_extensions = _get_core_extensions()

    disabled_core = []
    for key in core_extensions:
        if key in disabled:
            disabled_core.append(key)

    if disabled_core:
        logger.info('\nDisabled core extensions:')
        [logger.info('    %s' % item) for item in sorted(disabled_core)]


def clean(app_dir=None):
    """Clean the JupyterLab application directory."""
    app_dir = get_app_dir(app_dir)
    if app_dir == here:
        raise ValueError('Cannot clean the core app')
    for name in ['static', 'staging']:
        target = pjoin(app_dir, name)
        if osp.exists(target):
            shutil.rmtree(target)


def build(app_dir=None, name=None, version=None, logger=None):
    """Build the JupyterLab application.
    """
    func = partial(build_async, app_dir=app_dir, name=name, version=version,
                   logger=logger)
    return IOLoop.instance().run_sync(func)


@gen.coroutine
def build_async(app_dir=None, name=None, version=None, logger=None, abort_callback=None):
    """Build the JupyterLab application.
    """
    # Set up the build directory.
    logger = logger or logging
    app_dir = get_app_dir(app_dir)

    # Set up the build directory.
    yield check_node()
    if app_dir == here:
        raise ValueError('Cannot build extensions in the core app')

    _ensure_package(app_dir, name=name, version=version, logger=logger)
    staging = pjoin(app_dir, 'staging')

    extensions = _get_extensions(app_dir)

    # Ensure an empty linked_packages directory
    linked_packages = pjoin(staging, 'linked_packages')
    if osp.exists(linked_packages):
        shutil.rmtree(linked_packages)
    os.makedirs(linked_packages)

    # Install the linked packages.
    for (name, path) in _get_linked_packages(app_dir, logger=logger).items():
        # Handle linked extensions.
        if name in extensions:
            yield install_extension_async(path, app_dir, abort_callback=abort_callback)
        # Handle linked packages that are not extensions.
        else:
            yield _install_linked_package(staging, name, path, logger, abort_callback=abort_callback)

    npm = get_npm_name()

    # Make sure packages are installed.
    yield run([npm, 'install'], cwd=staging, logger=logger, abort_callback=abort_callback)

    # Build the app.
    yield run([npm, 'run', 'build'], cwd=staging, logger=logger, abort_callback= abort_callback)

    # Move the app to the static dir.
    static = pjoin(app_dir, 'static')
    if os.path.exists(static):
        shutil.rmtree(static)
    shutil.copytree(pjoin(app_dir, 'staging', 'build'), static)


@gen.coroutine
def _install_linked_package(staging, name, path, logger, abort_callback=None):
    """Install a linked non-extension package using a package tarball
    to prevent it from being treated as a symlink.
    """
    # Remove any existing package from staging/node_modules
    target = pjoin(staging, 'node_modules', name)
    target = target.replace('/', os.sep)
    if os.path.exists(target):
        shutil.rmtree(target)

    linked = pjoin(staging, 'linked_packages')

    target = pjoin(linked, 'temp')
    if os.path.exists(target):
        shutil.rmtree(target)
    os.makedirs(target)

    # npm pack the extension
    yield run([get_npm_name(), 'pack', path], cwd=target, logger=logger, abort_callback=abort_callback)

    fname = os.path.basename(glob.glob(pjoin(target, '*.*'))[0])
    data = _read_package(pjoin(target, fname))

    # Remove the tarball if the package is not compatible.
    core_data = _get_core_data()
    deps = data.get('dependencies', dict())
    errors = _validate_compatibility(path, deps, core_data)
    if errors:
        shutil.rmtree(target)
        msg = _format_compatibility_errors(
            data['name'], data['version'], errors
        )
        raise ValueError(msg)

    # Remove an existing extension tarball.
    ext_path = pjoin(linked, fname)
    if os.path.exists(ext_path):
        os.remove(ext_path)

    # Move
    shutil.move(pjoin(target, fname), linked)
    shutil.rmtree(target)


def _get_build_config(app_dir):
    """Get the build config data for the given app dir
    """
    target = pjoin(app_dir, 'settings', 'build_config.json')
    if not os.path.exists(target):
        return {}
    else:
        with open(target) as fid:
            return json.load(fid)


def _get_page_config(app_dir):
    """Get the page config data for the given app dir
    """
    target = pjoin(app_dir, 'settings', 'page_config.json')
    if not os.path.exists(target):
        return {}
    else:
        with open(target) as fid:
            return json.load(fid)


def _validate_compatibility(extension, deps, core_data):
    """Validate the compatibility of an extension.
    """
    core_deps = core_data['dependencies']
    singletons = core_data['jupyterlab']['singletonPackages']

    errors = []

    for (key, value) in deps.items():
        if key in singletons:
            overlap = _test_overlap(core_deps[key], value)
            if overlap is False:
                errors.append((key, core_deps[key], value))

    return errors


def _get_core_data():
    """Get the data for the app template.
    """
    with open(pjoin(here, 'package.app.json')) as fid:
        return json.load(fid)


def _test_overlap(spec1, spec2):
    """Test whether two version specs overlap.

    Returns `None` if we cannot determine compatibility,
    otherwise whether there is an overlap
    """
    # Test for overlapping semver ranges.
    r1 = Range(spec1, True)
    r2 = Range(spec2, True)

    # If either range is empty, we cannot verify.
    if not r1.range or not r2.range:
        return

    x1 = r1.set[0][0].semver
    x2 = r1.set[0][-1].semver
    y1 = r2.set[0][0].semver
    y2 = r2.set[0][-1].semver

    o1 = r1.set[0][0].operator
    o2 = r2.set[0][0].operator

    # We do not handle (<) specifiers.
    if (o1.startswith('<') or o2.startswith('<')):
        return

    # Handle single value specifiers.
    lx = lte if x1 == x2 else lt
    ly = lte if y1 == y2 else lt
    gx = gte if x1 == x2 else gt
    gy = gte if x1 == x2 else gt

    # Handle unbounded (>) specifiers.
    def noop(x, y, z):
        return True

    if x1 == x2 and o1.startswith('>'):
        lx = noop
    if y1 == y2 and o2.startswith('>'):
        ly = noop

    # Check for overlap.
    return (
        gte(x1, y1, True) and ly(x1, y2, True) or
        gy(x2, y1, True) and ly(x2, y2, True) or
        gte(y1, x1, True) and lx(y1, x2, True) or
        gx(y2, x1, True) and lx(y2, x2, True)
    )


def _format_compatibility_errors(name, version, errors):
    """Format a message for compatibility errors.
    """
    msgs = []
    l0 = 10
    l1 = 10
    for error in errors:
        pkg, jlab, ext = error
        jlab = str(Range(jlab, True))
        ext = str(Range(ext, True))
        msgs.append((pkg, jlab, ext))
        l0 = max(l0, len(pkg) + 1)
        l1 = max(l1, len(jlab) + 1)

    msg = '\n"%s@%s" is not compatible with the current JupyterLab'
    msg = msg % (name, version)
    msg += '\nConflicting Dependencies:\n'
    msg += 'JupyterLab'.ljust(l0)
    msg += 'Extension'.ljust(l1)
    msg += 'Package\n'

    for (pkg, jlab, ext) in msgs:
        msg += jlab.ljust(l0) + ext.ljust(l1) + pkg + '\n'

    return msg


def _toggle_extension(extension, value, app_dir=None, logger=None):
    """Enable or disable a lab extension.
    """
    app_dir = get_app_dir(app_dir)
    config = _get_page_config(app_dir)
    extensions = _get_extensions(app_dir)
    core_extensions = _get_core_extensions()

    if extension not in extensions and extension not in core_extensions:
        raise ValueError('Extension %s is not installed' % extension)
    disabled = config.get('disabledExtensions', [])
    if value and extension not in disabled:
        disabled.append(extension)
    if not value and extension in disabled:
        disabled.remove(extension)

    # Prune extensions that are not installed.
    disabled = [ext for ext in disabled
                if (ext in extensions or ext in core_extensions)]
    config['disabledExtensions'] = disabled
    _write_page_config(config, app_dir, logger=logger)


def _write_build_config(config, app_dir, logger):
    """Write the build config to the app dir.
    """
    _ensure_app_dirs(app_dir, logger)
    target = pjoin(app_dir, 'settings', 'build_config.json')
    with open(target, 'w') as fid:
        json.dump(config, fid, indent=4)


def _write_page_config(config, app_dir, logger):
    """Write the build config to the app dir.
    """
    _ensure_app_dirs(app_dir, logger)
    target = pjoin(app_dir, 'settings', 'page_config.json')
    with open(target, 'w') as fid:
        json.dump(config, fid, indent=4)


def _ensure_package(app_dir, logger=None, name=None, version=None):
    """Make sure the build dir is set up.
    """
    logger = logger or logging
    version = version or __version__
    _ensure_app_dirs(app_dir, logger)

    # Look for mismatched version.
    staging = pjoin(app_dir, 'staging')
    pkg_path = pjoin(staging, 'package.json')
    version_updated = False
    if os.path.exists(pkg_path):
        with open(pkg_path) as fid:
            data = json.load(fid)
        if data['jupyterlab'].get('version', '') != version:
            shutil.rmtree(staging)
            os.makedirs(staging)
            version_updated = True

    for fname in ['index.app.js', 'webpack.config.js']:
        dest = pjoin(staging, fname.replace('.app', ''))
        shutil.copy(pjoin(here, fname), dest)

    # Template the package.json file.
    data = _get_package_template(app_dir, logger)
    data['jupyterlab']['name'] = name or 'JupyterLab'
    data['jupyterlab']['version'] = version

    pkg_path = pjoin(staging, 'package.json')
    with open(pkg_path, 'w') as fid:
        json.dump(data, fid, indent=4)

    # Copy any missing or outdated schema or theme items.
    for item in ['schemas', 'themes']:
        local = pjoin(here, item)
        if not os.path.exists(local):
            os.makedirs(local)

        for item_path in os.listdir(local):
            src = pjoin(local, item_path)
            dest = pjoin(app_dir, item, item_path)
            if version_updated or not os.path.exists(dest):
                if os.path.isdir(src):
                    if os.path.exists(dest):
                        shutil.rmtree(dest)
                    shutil.copytree(src, dest)
                else:
                    shutil.copy(src, dest)


def _ensure_app_dirs(app_dir, logger):
    """Ensure that the application directories exist"""
    dirs = ['extensions', 'settings', 'schemas', 'themes', 'staging']
    for dname in dirs:
        path = pjoin(app_dir, dname)
        if not osp.exists(path):
            try:
                os.makedirs(path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise


def _get_package_template(app_dir, logger):
    # Get the template the for package.json file.
    data = _get_core_data()
    extensions = _get_extensions(app_dir)

    # Handle extensions
    for (key, value) in extensions.items():
        # Reject incompatible extensions with a message.
        deps = value.get('dependencies', dict())
        errors = _validate_compatibility(key, deps, data)
        if errors:
            msg = _format_compatibility_errors(key, value['version'], errors)
            logger.warn(msg + '\n')
            continue
        data['dependencies'][key] = value['path']
        jlab_data = value['jupyterlab']
        for item in ['extension', 'mimeExtension']:
            ext = jlab_data.get(item, False)
            if not ext:
                continue
            if ext is True:
                ext = ''
            data['jupyterlab'][item + 's'][key] = ext

    # Handle linked packages.
    linked = _get_linked_packages(app_dir, logger)
    for (key, path) in linked.items():
        if key in extensions:
            continue
        data['dependencies'][key] = path

    # Handle uninstalled core extensions.
    for item in _get_uinstalled_core_extensions(app_dir):
        if item in data['jupyterlab']['extensions']:
            data['jupyterlab']['extensions'].pop(item)
        else:
            data['jupyterlab']['mimeExtensions'].pop(item)

    return data


def _is_extension(data):
    """Detect if a package is an extension using its metadata.
    """
    if 'jupyterlab' not in data:
        return False
    if not isinstance(data['jupyterlab'], dict):
        return False
    is_extension = data['jupyterlab'].get('extension', False)
    is_mime_extension = data['jupyterlab'].get('mimeExtension', False)
    return is_extension or is_mime_extension


def _get_uinstalled_core_extensions(app_dir):
    """Get the uninstalled core extensions.
    """
    config = _get_build_config(app_dir)
    return config.get('uninstalled_core_extensions', [])


def _validate_package(data, extension):
    """Validate package.json data.
    """
    msg = '%s is not a valid JupyterLab extension' % extension
    if not _is_extension(data):
        raise ValueError(msg)


def _get_disabled(app_dir):
    """Get the disabled extensions.
    """
    config = _get_page_config(app_dir)
    return config.get('disabledExtensions', [])


def _get_core_extensions():
    """Get the core extensions.
    """
    data = _get_core_data()['jupyterlab']
    return list(data['extensions']) + list(data['mimeExtensions'])


def _get_extensions(app_dir):
    """Get the extensions in a given app dir.
    """
    extensions = dict()

    # Get system level packages
    sys_path = pjoin(get_app_dir(), 'extensions')
    app_path = pjoin(app_dir, 'extensions')
    for target in glob.glob(pjoin(sys_path, '*.tgz')):
        location = 'app' if app_path == sys_path else 'system'
        data = _read_package(target)
        deps = data.get('dependencies', dict())
        extensions[data['name']] = dict(path=os.path.realpath(target),
                                        version=data['version'],
                                        jupyterlab=data['jupyterlab'],
                                        dependencies=deps,
                                        location=location)

    # Look in app_dir if different
    app_path = pjoin(app_dir, 'extensions')
    if app_path == sys_path or not os.path.exists(app_path):
        return extensions

    for target in glob.glob(pjoin(app_path, '*.tgz')):
        data = _read_package(target)
        deps = data.get('dependencies', dict())
        extensions[data['name']] = dict(path=os.path.realpath(target),
                                        version=data['version'],
                                        jupyterlab=data['jupyterlab'],
                                        dependencies=deps,
                                        location='app')

    return extensions


def _get_linked_packages(app_dir=None, logger=None):
    """Get the linked packages metadata.
    """
    logger = logger or logging
    app_dir = get_app_dir(app_dir)
    config = _get_build_config(app_dir)
    linked = config.get('linked_packages', dict())
    dead = []
    for (name, path) in linked.items():
        if not os.path.exists(path):
            dead.append(name)

    if dead:
        extensions = _get_extensions(app_dir)

    for name in dead:
        path = linked[name]
        if name in extensions:
            uninstall_extension(name)
            logger.warn('**Note: Removing dead linked extension "%s"' % name)
        else:
            logger.warn('**Note: Removing dead linked package "%s"' % name)
        del linked[name]

    if dead:
        config['linked_packages'] = linked
        _write_build_config(config, app_dir, logger=logger)

    return config.get('linked_packages', dict())


def _read_package(target):
    """Read the package data in a given target tarball.
    """
    tar = tarfile.open(target, "r:gz")
    f = tar.extractfile('package/package.json')
    data = json.loads(f.read().decode('utf8'))
    tar.close()
    return data


def _copy_tar_files(fname, source, dest):
    """Copy the files from a target path to the destination.
    """
    tar = tarfile.open(fname, "r:gz")
    subdir_and_files = [
        tarinfo for tarinfo in tar.getmembers()
        if tarinfo.name.startswith('package/' + source)
    ]
    offset = len('package/' + source + '/')
    for member in subdir_and_files:
        member.path = member.path[offset:]
    tar.extractall(path=dest, members=subdir_and_files)
    tar.close()


def _normalize_path(extension):
    """Normalize a given extension if it is a path.
    """
    extension = osp.expanduser(extension)
    if osp.exists(extension):
        extension = osp.abspath(extension)
    return extension
