#!/usr/bin/env python

from pathlib import Path
import tempfile

import click
from loguru import logger
import yaml

from dagster.cli import cli as dagster_cli
from dagit.cli import cli as dagit_cli
from ..dagster.resources import _make_resources_config_dict


def _make_dagster_config_from_dvp_config(dvp_config):
    cfg = dvp_config.dagster.copy()
    cfg['resources'].update(_make_resources_config_dict(dvp_config))
    return cfg


def make_dagster_command():
    # this takes the dagster cli commands and compose our own
    # command group

    def config_callback(ctx, param, value):
        # TODO
        # maybe we change this to properly define the commands
        # on our own
        return (ctx.find_root()._dagster_config_path.as_posix(), ) + value

    def patch_cmd(c):
        if isinstance(c, click.Group):
            for cc in c.commands.values():
                patch_cmd(cc)
            return
        for param in c.params:
            if param.name == 'config':
                param.default = ()
                param.callback = config_callback
                return

    patch_cmd(dagster_cli)

    @click.group(
        'dagster',
        commands=dagster_cli.commands,
        context_settings=dagster_cli.context_settings,
        invoke_without_command=True
        )
    @click.pass_context
    def cmd_dagster(ctx, ):
        # compose the resource config dict for dvpipe_config resource
        # this has to be written to a temp file
        dagster_config = _make_dagster_config_from_dvp_config(ctx.obj.dvpipe)
        tempdir = ctx.with_resource(tempfile.TemporaryDirectory())
        fname = Path(tempdir).joinpath('dagster_config.yaml')
        with open(fname, 'w') as fo:
            yaml.safe_dump(dagster_config, fo)
        logger.debug(f"generated dagster config filepath: {fname}")
        with open(fname, 'r') as fo:
            logger.debug(f'config:\n{fo.read()}')
        ctx.find_root()._dagster_config_path = fname

    return cmd_dagster


def make_dagit_command():
    return dagit_cli
