#!/usr/bin/env python

from loguru import logger

from astropy.table import Table

from pyDataverse.models import Dataset as _DVDataset
from pyDataverse.models import Datafile as _DVDatafile

import json
from .utils import pformat_resp, pformat_yaml


class DVDataset(_DVDataset):
    """A class to handle dataverse dataset.

    This enables the handling of extra metadata block.
    """
    def json(self, **kwargs):
        logger.debug(f"generate json for standard dataset")
        data = json.loads(super().json(**kwargs))
        for key, item in self.get().get('metadata_blocks', {}).items():
            logger.debug(f"generate json for custom metadata block {key}")
            data["datasetVersion"]["metadataBlocks"][key] = item
        return json.dumps(data, indent=2)


class DVDatafile(_DVDatafile):
    """A class to handle dataverse data files.
    """

def search_dataverse(dv_config, **kwargs):
    """Search the dataverse.

    Parameters
    ----------
    dv_config : dvpipe.core.DataverseConfig
        The dataverse connection config.
    **kwargs :
        The arguments passed to `pyDataverse.SearchApi.search`.

    Returns
    -------
    df : The data frame containing the result.
    """
    logger.debug(f'search dataverse with kwargs:\n{pformat_yaml(kwargs)}')

    api = dv_config.search_api
    resp = api.search(**kwargs)
    logger.debug(f'search query:\n{pformat_resp(resp)}')
    data = resp.json().pop('data')
    items = data.pop('items')
    logger.debug(f'metadata:\n{data}')
    if not items:
        # we return an empty table to hold the metadata anyway
        tbl = Table()
    else:
        tbl = Table(rows=items, dtype=[object] * len(items[0]))
    tbl.meta['response_data'] = data
    return tbl


def get_datafiles(dv_config, dataset_id, version=':latest'):
    """Return the files in dataset.

    Parameters
    ----------
    dv_config : dvpipe.core.DataverseConfig
        The dataverse connection config.
    dataset_id : str
        The persistent id of the dataset.
    version : str, optional
        The vresion of the dataset. Default is ':latest'
    Returns
    -------
    df : The data frame containing the result.
    """
    logger.debug(f'get file list of dataset pid={dataset_id}')

    api = dv_config.native_api
    url = (
        f'{api.base_url_api}/datasets/:persistentId/versions/'
        f'{version}/files?persistentId={dataset_id}'
        )
    resp = api.get_request(url, auth=True)
    logger.debug(f'data files query:\n{pformat_resp(resp)}')
    data = resp.json()
    items = data.pop('data')
    logger.debug(f'metadata:\n{data}')
    if not items:
        # we return an empty table to hold the metadata anyway
        tbl = Table()
    else:
        tbl = Table(rows=items, dtype=[object] * len(items[0]))
    tbl.meta['response_data'] = data
    return tbl


def upload_dataset(
        dv_config, parent_id, dataset_index,
        action_on_exist='none',
        publish_type='none'):
    """Upload dataset to dataverse.

    Parameters
    ----------
    dv_config : dvpipe.core.DataverseConfig
        The dataverse connection config.
    parent_id : str
        The identifier of the parent dataverse.
    dataset_index : dict
        The index file of the dataset to be uploaded.
    action_on_exist : {'none', 'update', 'create'}
        The action to take when the dataset exists:
        * 'none': no action.
        * 'update': update existing dataset.
        * 'create': create new dataset.
    publish_type : {'none', 'major', 'minor'}
        How the dataset is published:
        * 'none': do not publish.
        * 'major': publish with major version bump.
        * 'minor': publish with minor version bump.
    """
    api = dv_config.native_api
    # parent info
    resp = api.get_dataverse(parent_id)
    logger.info(f'query parent dataverse:\n{pformat_resp(resp)}')
    parent_meta = resp.json().pop("data")
    logger.debug(
        f"parent dataverse meta:\n{pformat_yaml(parent_meta)}")
    logger.debug(f"dataset index meta:\n{pformat_yaml(dataset_index['meta'])}")

    # create ds object
    ds = DVDataset()
    ds.set(dataset_index['dataset'])
    # assert ds.validate_json()
    logger.debug(f"dataset json:\n{ds.json()}")

    def _create():
        resp = api.create_dataset(
            parent_id, ds.json(), pid=None, publish=False, auth=True)
        logger.info(f"create dataset:\n{pformat_resp(resp)}")
        # get pid
        return resp.json()["data"]["persistentId"]

    if action_on_exist == 'create':
        # just create
        pid = _create()
    else:
        # search for existing dataset
        search_kwargs = {
            'q_str': f'title:{dataset_index["dataset"]["title"]}',
            'subtree': parent_id,
            'sort': 'date',
            'order': 'desc'
            }
        results = search_dataverse(dv_config=dv_config, **search_kwargs)
        if not results:
            # not exist, create
            logger.debug(
                f"no dataset found with "
                f"search_kwargs:\n{pformat_yaml(search_kwargs)}")
            pid = _create()
        else:
            # warn if multiple entries found
            if len(results) > 1:
                entry_info = dict(zip(results.colnames, results[0]))
                logger.warning(
                    f"multiple entries found in search:\n{results}\n"
                    f"use the latest entry:\n{pformat_yaml(entry_info)}"
                    )
            # get the latest dataset pid
            pid = results[0]['global_id']
            if action_on_exist == 'none':
                # nothing need to be done
                logger.debug(
                    f"action_on_exist is none and dataset exists pid={pid},"
                    f" nothing to do.")
                return pid
            if action_on_exist == 'update':
                logger.debug("update dataset with metadata")
                # TODO implement this
                # update dataset with pid
            else:
                raise ValueError("invalid action.")
    # if we reach here, we need to handle the datafiles
    logger.info(f"upload datafiles to dataset pid: {pid}")
    data_files = list()
    # we retrieve the list of data files in the dataset
    # if action is to update.
    if action_on_exist == 'update':
        files_remote = get_datafiles(dv_config, dataset_id=pid)
        logger.debug(f"existing files:\n{files_remote}")

    for data in dataset_index['files']:
        # update pid to point to the dataset.
        data['pid'] = pid
        df = DVDatafile()
        df.set(data)
        assert df.validate_json()
        data_files.append(df)
        # upload file if needed
        if action_on_exist == 'create':
            resp = api.upload_datafile(df.pid, df.filename, df.json())
            logger.info(f"create datafile:\n{pformat_resp(resp)}")
        elif action_on_exist == 'update':
            # check if the file is in the list of existing files
            m = files_remote[files_remote['label'] == df.label]
            if len(m) > 0:
                if len(m) > 1:
                    logger.warning(
                        f"multiple files found with label={df.label}"
                        )
                # TODO implement checksum comparison to do update?
                # file_pid = m[0]['dataFile']['id']
                # resp = api.replace_datafile(
                #     file_pid, df.filename, df.json(), is_filepid=False)
                # logger.info(
                #     f"update existing datafile:\n{pformat_resp(resp)}")
                logger.info(
                    f"skip existing datafile label={df.label}")
            else:
                # not exist yet, create
                resp = api.upload_datafile(df.pid, df.filename, df.json())
                logger.info(
                    f"create non-existing datafile:\n{pformat_resp(resp)}")
        else:
            raise ValueError("invalid action")
    # finally, publish the dataset if requested
    if publish_type in ['major', 'minor']:
        resp = api.publish_dataset(pid, publish_type)
        logger.info(f'publish dataset pid={pid}: {pformat_resp(resp)}')
    return pid
