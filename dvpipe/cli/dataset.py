#!/usr/bin/env python

import click
import pandas as pd
from loguru import logger

from ..dataverse import search_dataverse, upload_dataset
from ..utils import pformat_resp, pformat_yaml, yaml


@click.group(
    'dataset',
    )
@click.pass_obj
def cmd_dataset(ctxobj, ):
    """The CLI for dataset management."""
    pass


@cmd_dataset.command('list')
@click.option(
    '--parent', '-p',
    default=':root',
    metavar='ID',
    help='The id of parent dataverse.',
    )
@click.pass_obj
def cmd_dataset_list(ctxobj, parent):
    """List all datasets in a dataverse."""
    api = ctxobj.dvpipe.dataverse.native_api
    # parent info
    resp = api.get_dataverse(parent)
    logger.debug(f'query parent dataverse:\n{pformat_resp(resp)}')
    data = resp.json().pop("data")
    logger.debug(f"parent dataverse metadata:\n{pformat_yaml(data)}")
    # get children
    datasets = api.get_children(
        parent=parent, parent_type='dataverse',
        children_types=['datasets', ])
    df = pd.DataFrame.from_records(datasets)
    if len(df) == 0:
        print(f"No dataset found in parent {parent}")
    else:
        print(df)


@cmd_dataset.command('search')
@click.option(
    '--action_on_exist', '-a',
    type=click.Choice(
        ['none', 'delete'],
        case_sensitive=False),
    default='none',
    help='The action to take for returned datasets',
    )
@click.argument(
    'options', nargs=-1,
    metavar='OPT',
    )
@click.pass_obj
def cmd_dataset_search(ctxobj, action_on_exist, options):
    """Search the dataverse."""
    # build the options dict
    kwargs = {
        'q_str': '*',
        'data_type': 'dataset',
        }
    # TODO the pydataverse current does not support
    # query repeatable fields.
    # _repeatable_keys = ['subtree', 'metadata_fields']
    _repeatable_keys = []
    for kv in options:
        k, v = kv.split('=', 1)
        if k in _repeatable_keys:
            v = [v]
        # values for repeated keys get merged
        if k in kwargs and k in _repeatable_keys:
            kwargs[k].extend(v)
        else:
            kwargs[k] = v
    result = search_dataverse(ctxobj.dvpipe.dataverse, **kwargs)
    if result:
        print(result)
    else:
        print(
            f"No result found. Details:"
            f"\n{pformat_yaml(result.meta)}"
            )
    # handle action
    if action_on_exist == "none":
        return
    if action_on_exist == "delete":
        api = ctxobj.dvpipe.dataverse.native_api
        for item in result:
            item_str = pformat_yaml(dict(item))
            if click.confirm(f'Deleting dataset\n{item_str}\nDo you want to continue?'):
                api.delete_dataset(item['global_id'], is_pid=True, auth=True)
        


@cmd_dataset.command('upload')
@click.option(
    '--parent', '-p',
    default=':root',
    metavar='ID',
    help='The id of parent dataverse.',
    )
@click.option(
    '--index_file', '-i',
    type=click.Path(
        exists=True,
        file_okay=True, dir_okay=False,
        readable=True),
    default=None,
    metavar='FILE',
    help='YAML file path that defines the dataset content.',
    )
@click.option(
    '--action_on_exist', '-a',
    type=click.Choice(
        ['none', 'update', 'create'],
        case_sensitive=False),
    default='none',
    help='The action to take when the dataset exists',
    )
@click.option(
    '--publish_type', '-b',
    type=click.Choice(
        ['none', 'major', 'minor'],
        case_sensitive=False),
    default='none',
    help='Specify how the dataset is published ("none" for not publish).',
    )
@click.pass_obj
def cmd_dataset_upload(
        ctxobj, parent, index_file, action_on_exist, publish_type):
    """Create dataset in `parent` according to the content of `index_file`."""
    with open(index_file, 'r') as fo:
        dataset_index = yaml.load(fo)
    dataset_meta = dataset_index['meta']
    logger.info(f"upload dataset:\n{pformat_yaml(dataset_meta)}")
    upload_dataset(
        ctxobj.dvpipe.dataverse,
        parent_id=parent,
        dataset_index=dataset_index,
        action_on_exist=action_on_exist,
        publish_type=publish_type)
