#!/usr/bin/env python

import click
import pandas as pd
from loguru import logger

from astropy.io.misc import yaml

from pyDataverse.models import Dataset as DVDataset
from pyDataverse.models import Datafile as DVDatafile


from ..utils import pformat_resp, pformat_yaml


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
    api = ctxobj.dvpipe.dataverse.api
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
@click.pass_obj
def cmd_dataset_upload(ctxobj, parent, index_file):
    """Create dataset in `parent` according to the content of `index_file`."""
    api = ctxobj.dvpipe.dataverse.api
    # parent info
    resp = api.get_dataverse(parent)
    logger.debug(f'query parent dataverse:\n{pformat_resp(resp)}')
    data = resp.json().pop("data")
    logger.debug(f"parent dataverse metadata:\n{pformat_yaml(data)}")

    with open(index_file, 'r') as fo:
        dataset_index = yaml.load(fo)
    logger.info(f"load dataset meta:\n{pformat_yaml(dataset_index['meta'])}")
    # create ds
    ds = DVDataset()
    ds.set(dataset_index['dataset'])
    assert ds.validate_json()

    # create dataset
    resp = api.create_dataset(
        parent, ds.json(), pid=None, publish=False, auth=True)
    logger.info(f"create dataset:\n{pformat_resp(resp)}")
    pid = resp.json()["data"]["persistentId"]
    logger.debug(f"dataset pid: {pid}")

    data_files = list()
    for data in dataset_index['files']:
        # update pid to point to the dataset.
        data['pid'] = pid
        df = DVDatafile()
        df.set(data)
        assert df.validate_json()
        data_files.append(df)

    # TODO
    # implement the conversion between index and json data for dataset
    # creation.
    # some snippets can be found here:
    # https://pydataverse.readthedocs.io/en/latest/user/advanced-usage.html#advanced-usage-data-migration
    # 1. create dataset with metadata and get pid
    # resp = api.create_dataset(
    #     parent, metadata, pid=None, publish=False, auth=True)
    # pid = resp.json()["data"]["persistentId"]
    # 2. loop over datafiles to create data files:
    # for df in df_lst:
    #     pid = dataset_id_2_pid[df.get()["org.dataset_id"]]
    #     filename = os.path.join(os.getcwd(), df.get()["org.filename"])
    #     df.set({"pid": pid, "filename": filename})
    #     resp = api.upload_datafile(pid, filename, df.json())
    # 3. publish dataset specified by the pid
    # resp = api.publish_dataset(pid, "major")
    print("Import not implemented yet.")
