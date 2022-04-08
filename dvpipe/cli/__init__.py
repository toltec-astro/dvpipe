#!/usr/bin/env python

import sys
import click
from loguru import logger
import yaml
from pydantic import BaseModel
from typing import Optional
from pathlib import Path
from wrapt import ObjectProxy

from .. import __version__
from ..core import DVPConfig
from ..utils import pformat_yaml


ctxobj_proxy = ObjectProxy(None)
"""A proxy object of the current click context object.
"""


_prog_info = {
    'name': 'dvpipe',
    'version': __version__,
    'desc': 'Dataverse integration for data pipelines.',
    'banner': r'''
 ____  _     ____  _  ____  _____
/  _ \/ \ |\/  __\/ \/  __\/  __/
| | \|| | //|  \/|| ||  \/||  \
| |_/|| \// |  __/| ||  __/|  /_
\____/\__/  \_/   \_/\_/   \____\

'''
    }


def _make_help_text():
    return """{name} v{version} - {desc}""".format(**_prog_info)


def _init_log(level='INFO'):
    # remove the pre-configured logger
    logger.remove(0)
    logger.add(sys.stderr, level=level)


class Config(BaseModel):
    """A wrapper model for loading configs from config file."""

    dvpipe: DVPConfig
    config_file: Optional[Path]


@click.group(
    context_settings={
        'help_option_names': ('-h', '--help'),
        },
    help=_make_help_text(),
    invoke_without_command=True,
    )
@click.version_option(
    __version__,
    '-v', '--version',
    message='%(version)s',
    )
@click.option(
    '--debug', '-g',
    is_flag=True,
    default=False,
    help='Enable debug messages.',
    )
@click.option(
    '--no_banner',
    is_flag=True,
    default=False,
    help='Disable banner.',
    )
@click.option(
    '--config_file', '-c',
    type=click.Path(
        exists=True,
        file_okay=True, dir_okay=False,
        readable=True),
    default=None,
    metavar='FILE',
    help='YAML file path to load config from.',
    )
@click.option(
    '--env_file', '-e',
    type=click.Path(
        exists=False,
        file_okay=True, dir_okay=False,
        readable=True),
    default=DVPConfig.Config.env_file,
    metavar='FILE',
    help='Dotenv file path to load env vars from.',
    )
@click.pass_context
def main(ctx, debug, no_banner, config_file, env_file):
    """The CLI entry point."""

    # show banner
    if not no_banner:
        print(_prog_info['banner'])

    _init_log(level='DEBUG' if debug else 'INFO')

    if config_file is None:
        config = {
            'dvpipe': {}
            }
    else:
        with open(config_file, 'r') as fo:
            config = yaml.safe_load(fo)
    # update the env file for dvpconfig to pick it up
    if env_file is not None:
        DVPConfig.Config.env_file = env_file
    ctxobj = ctx.obj = ctxobj_proxy.__wrapped__ = Config(
        **config, config_file=config_file)
    logger.debug(f"dvp cfg:\n{ctxobj.dvpipe.yaml()}")
    # try show the serve info version
    api = ctxobj.dvpipe.dataverse.api
    resp = api.get_info_version()
    if resp.ok:
        logger.info(f"Dataverse server info:\n{pformat_yaml(resp.json())}")
    else:
        logger.error("Unable to connect to dataverse server.")
    return


# ################################################
# sub commands
from .user import cmd_user  # noqa: E402
main.add_command(cmd_user)
from .dataset import cmd_dataset  # noqa: E402
main.add_command(cmd_dataset)
from .dagster import make_dagit_command, make_dagster_command  # noqa: E402
main.add_command(make_dagit_command())
main.add_command(make_dagster_command())
# ################################################


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
