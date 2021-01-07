# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright (c) 2016-2017 Anaconda, Inc.
#
# May be copied and distributed freely only as part of an Anaconda or
# Miniconda installation.
# -----------------------------------------------------------------------------
"""
Module in charge of the configuration settings.

It uses a modified version of Python's configuration parser.
"""

# yapf: disable

# Standard library imports
import os
import sys

# Local imports
from anaconda_navigator import __version__
from anaconda_navigator.config.base import (get_conf_path, get_home_dir,
                                            is_gtk_desktop, is_ubuntu)
from anaconda_navigator.config.main import CONF


# yapf: enable

# FLAGS
CONF
TEST_CI = os.environ.get('TEST_CI', False)
MAC = sys.platform == 'darwin'
WIN = os.name == 'nt'
LINUX = sys.platform.startswith('linux')
UBUNTU = is_ubuntu()
GTK = is_gtk_desktop()
DEV = 'dev' in __version__

HOME_PATH = get_home_dir()
CONF_PATH = get_conf_path()
LAUNCH_SCRIPTS_PATH = os.path.join(CONF_PATH, 'scripts')
CONTENT_PATH = os.path.join(CONF_PATH, 'content')
CONTENT_JSON_PATH = os.path.join(CONTENT_PATH, 'content.json')
IMAGE_ICON_SIZE = (256, 256)
IMAGE_DATA_PATH = os.path.join(CONF_PATH, 'images')
CHANNELS_PATH = os.path.join(CONF_PATH, 'channels')
METADATA_PATH = os.path.join(CONF_PATH, 'metadata')
DEFAULT_PROJECTS_PATH = os.path.join(HOME_PATH, 'AnacondaProjects')
LOCKFILE = os.path.join(CONF_PATH, 'navigator.lock')
PIDFILE = os.path.join(CONF_PATH, 'navigator.pid')
DEFAULT_BRAND = 'Anaconda Cloud'

# License management
LICENSE_PATH = '__filepath__'
REMOVED_LICENSE_PATH = '.removed_licenses'
# Other license like cluster dont really have a meaning for a dekstop user
# using navigator
VALID_PRODUCT_LICENSES = [
    'accelerate',
    'Anaconda Enterprise Notebooks',
    'Anaconda Enterprise Repository',
    'Anaconda Repository Enterprise',
    'Anaconda Enterprise',
    'Wakari',
    'iopro',
    'mkl-optimizations',
]
PACKAGES_WITH_LICENSE = [
    'anaconda-fusion',
    'anaconda-mosaic',
]
LICENSE_NAME_FOR_PACKAGE = {
    'anaconda-fusion': [
        'Anaconda Enterprise Notebooks',
        'Anaconda Enterprise Repository',
        'Anaconda Repository Enterprise',
        'Anaconda Enterprise',
        'Wakari',
        'Wakari Enterprise',
    ],
}

VALID_DEV_TOOLS = ['notebook', 'qtconsole', 'spyder']
LOG_FOLDER = os.path.join(CONF_PATH, 'logs')
LOG_FILENAME = 'navigator.log'

MAX_LOG_FILE_SIZE = 2 * 1024 * 1024
