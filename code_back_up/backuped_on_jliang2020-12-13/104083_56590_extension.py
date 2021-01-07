# coding: utf-8
"""A tornado based Jupyter lab server."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import os

from jupyterlab_launcher import add_handlers, LabConfig
from notebook.utils import url_path_join as ujoin

from notebook.base.handlers import FileFindHandler


from .commands import (
    get_app_dir, list_extensions, should_build, get_user_settings_dir
)

from .build_handler import build_path, Builder, BuildHandler
from ._version import __version__

#-----------------------------------------------------------------------------
# Module globals
#-----------------------------------------------------------------------------

DEV_NOTE_NPM = """You're running JupyterLab from source.
If you're working on the TypeScript sources of JupyterLab, try running

    npm run watch

from the JupyterLab repo directory in another terminal window to have the
system incrementally watch and build JupyterLab's TypeScript for you, as you
make changes.
"""


CORE_NOTE = """
Running the core application with no additional extensions or settings
"""


def load_jupyter_server_extension(nbapp):
    """Load the JupyterLab server extension.
    """
    # Print messages.
    here = os.path.dirname(__file__)
    nbapp.log.info('JupyterLab alpha preview extension loaded from %s' % here)

    app_dir = get_app_dir()
    if hasattr(nbapp, 'app_dir'):
        app_dir = get_app_dir(nbapp.app_dir)

    web_app = nbapp.web_app
    config = LabConfig()

    config.assets_dir = os.path.join(app_dir, 'static')
    config.settings_dir = os.path.join(app_dir, 'settings')
    config.page_title = 'JupyterLab Alpha Preview'
    config.page_url = '/lab'
    config.dev_mode = False

    # Check for core mode.
    core_mode = ''
    if hasattr(nbapp, 'core_mode'):
        core_mode = nbapp.core_mode

    # Check for an app dir that is local.
    if app_dir == here or app_dir == os.path.join(here, 'build'):
        core_mode = True
        config.settings_dir = ''

    # Run core mode if explicit or there is no static dir and no
    # installed extensions.
    installed = list_extensions(app_dir)
    fallback = not installed and not os.path.exists(config.assets_dir)

    web_app.settings.setdefault('page_config_data', dict())
    web_app.settings['page_config_data']['buildAvailable'] = True
    web_app.settings['page_config_data']['token'] = nbapp.token

    if core_mode or fallback:
        config.assets_dir = os.path.join(here, 'build')
        config.version = __version__
        if not os.path.exists(config.assets_dir):
            msg = 'Static assets not built, please see CONTRIBUTING.md'
            nbapp.log.error(msg)
        else:
            sentinel = os.path.join(here, 'build', 'release_data.json')
            config.dev_mode = not os.path.exists(sentinel)

    if config.dev_mode:
        nbapp.log.info(DEV_NOTE_NPM)
    elif core_mode or fallback:
        nbapp.log.info(CORE_NOTE.strip())

    if core_mode or fallback:
        schemas_dir = os.path.join(here, 'schemas')
    else:
        schemas_dir = os.path.join(app_dir, 'schemas')

    config.schemas_dir = schemas_dir
    config.user_settings_dir = get_user_settings_dir()
    config.themes_dir = os.path.join(here, 'themes')

    add_handlers(web_app, config)

    base_url = web_app.settings['base_url']
    build_url = ujoin(base_url, build_path)
    builder = Builder(nbapp.log, core_mode, app_dir)
    build_handler = (build_url, BuildHandler, {'builder': builder})

    web_app.add_handlers(".*$", [build_handler])
