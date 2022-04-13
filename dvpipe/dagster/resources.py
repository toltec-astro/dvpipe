#!/usr/bin/env python

from dagster import resource
from ..core import DataverseConfig


@resource
def dataverse_config(init_context):
    """Dataverse API resource."""
    return DataverseConfig(**init_context.resource_config)


class _ResourceConfigMakers(object):

    @staticmethod
    def dataverse_config(dvp_config):
        # create config dict for source from dvp config
        return {
            'config': dvp_config.dataverse.dict()
            }


DVPIPE_RESOURCES = {
    'dataverse_config': dataverse_config
    }


def _make_resources_config_dict(dvp_config):

    _locals = locals()
    resources = dict()
    for name in DVPIPE_RESOURCES.keys():
        resources[name] = getattr(_ResourceConfigMakers, name)(dvp_config)
    return resources
