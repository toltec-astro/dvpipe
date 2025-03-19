#!/usr/bin/env python

import os
import zipfile
from loguru import logger

from astropy.table import Table
from copy import deepcopy
from dataclasses import dataclass

from pyDataverse.models import Dataset as _DVDataset
from pyDataverse.models import Datafile as _DVDatafile
from typing import Literal

import json
import numpy as np
from .utils import pformat_resp, pformat_yaml, yaml
from .core import DataverseConfig


# replace numpy.bool_ with bool
# see https://stackoverflow.com/questions/58408054/typeerror-object-of-type-bool-is-not-json-serializable
class CustomJSONizer(json.JSONEncoder):
    def default(self, obj):
        return (
            super().encode(bool(obj))
            if isinstance(obj, np.bool_)
            else super().default(obj)
        )


def _json_dumps(data):
    return json.dumps(
        data, indent=2, cls=CustomJSONizer, default=str, ensure_ascii=False
    )


class DVDataset(_DVDataset):
    """A class to handle dataverse dataset.

    This enables the handling of extra metadata block.
    """

    def json(self, **kwargs):
        logger.debug(f"generate json for standard dataset")
        data = json.loads(super().json(**kwargs))
        data["datasetVersion"].update(
            {
                "license": {
                    "name": "CC0 1.0",
                    "uri": "http://creativecommons.org/publicdomain/zero/1.0",
                    "iconUri": "https://licensebuttons.net/p/zero/1.0/88x31.png",
                },
                "termsOfAccess": "You need to request for access.",
                "fileAccessRequest": True,
            }
        )
        for key, item in self.get().get('metadata_blocks', {}).items():
            logger.debug(f"generate json for custom metadata block {key}")
            data["datasetVersion"]["metadataBlocks"][key] = item
        return json.dumps(
            data, indent=2, cls=CustomJSONizer, default=str, ensure_ascii=False
        )
        # return data_str.replace('\uFFFD', '?')


class DVDatafile(_DVDatafile):
    """A class to handle dataverse data files."""


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
    logger.debug(f"search dataverse with kwargs:\n{pformat_yaml(kwargs)}")

    api = dv_config.search_api
    resp = api.search(**kwargs)
    logger.debug(f"search query:\n{pformat_resp(resp)}")
    data = resp.json().pop("data")
    items = data.pop("items")
    logger.debug(f"metadata:\n{data}")
    if not items:
        # we return an empty table to hold the metadata anyway
        tbl = Table()
    else:
        tbl = Table(rows=items)  # , dtype=[object] * max(len(item) for item in items))
    tbl.meta["response_data"] = data
    return tbl


def get_datafiles(dv_config, dataset_id, version=":latest"):
    """Return the files in dataset.

    Parameters
    ----------
    dv_config : dvpipe.core.DataverseConfig
        The dataverse connection config.
    dataset_id : str
        The persistent id of the dataset.
    version : str, optional
        The version of the dataset. Default is ':latest'
    Returns
    -------
    df : The data frame containing the result.
    """
    logger.debug(f"get file list of dataset pid={dataset_id}")

    api = dv_config.native_api
    url = (
        f"{api.base_url_api}/datasets/:persistentId/versions/"
        f"{version}/files?persistentId={dataset_id}"
    )
    resp = api.get_request(url, auth=True)
    logger.debug(f"data files query:\n{pformat_resp(resp)}")
    data = resp.json()
    items = data.pop("data")
    logger.debug(f"metadata:\n{data}")
    if not items:
        # we return an empty table to hold the metadata anyway
        tbl = Table()
    else:
        tbl = Table(rows=items, dtype=[object] * len(items[0]))
    tbl.meta["response_data"] = data
    return tbl


def get_versions(dv_config, dataset_id, include_files=False, include_metadata=False):
    """Return the files in dataset.

    Parameters
    ----------
    dv_config : dvpipe.core.DataverseConfig
        The dataverse connection config.
    dataset_id : str
        The persistent id of the dataset.
    Returns
    -------
    df : The data frame containing the result.
    """
    logger.debug(f"get version list of dataset pid={dataset_id}")

    api = dv_config.native_api
    url = (
        f"{api.base_url_api}/datasets/:persistentId/versions"
        f"?persistentId={dataset_id}"
    )
    resp = api.get_request(url, auth=True)
    logger.debug(f"dataset version query:\n{pformat_resp(resp)}")
    data = resp.json()
    items = data.pop("data")
    for item in items:
        if not include_files:
            del item["files"]
        if not include_metadata:
            del item["metadataBlocks"]
    logger.debug(f"metadata:\n{data}")
    if not items:
        # we return an empty table to hold the metadata anyway
        tbl = Table()
    else:
        tbl = Table(rows=items)  # , dtype=[object] * len(items[0]))
    tbl.meta["response_data"] = data
    return tbl


@dataclass
class FileUploader:
    """A base class for file uploader."""
    dv_config: DataverseConfig 

       
@dataclass
class FileUploaderNative(FileUploader):
    """The file uploader that uses the native api."""

    def create(self, df):
        df = self._normalize_file(df)
        api = self.dv_config.native_api
        df_json = df.json()
        logger.debug(f"create file json:\n{df_json}")
        resp = api.upload_datafile(df.pid, df.filename, df_json)
        logger.info(f"create datafile:\n{pformat_resp(resp)}")
        return resp
       
    def replace(self, file_pid, df):
        df = self._normalize_file(df)
        api = self.dv_config.native_api
        df_json = df.json()
        logger.debug(f"replace file json:\n{df_json}")
        resp = api.replace_datafile(
            file_pid, df.filename, df_json, is_filepid=False)
        logger.info(
            f"overwrite existing datafile:\n{pformat_resp(resp)}")
        # update the restricted flag
        # this had to be done separately because the file replace
        # may fail
        url = f"{api.base_url_api_native}/files/{file_pid}/restrict"
        resp = api.put_request(url, auth=True, data=json.dumps(df.restrict))
        logger.info(
            f"update file restrict state:\n{pformat_resp(resp)}")

    @classmethod
    def _normalize_file(cls, df):
        filename = df.filename
        if filename.endswith(".zip"):
            df.filename = cls._zip_zip(filename)
        return df
       
    @staticmethod
    def _zip_zip(zip_path):
        if not zip_path.lower().endswith('.zip') or not os.path.isfile(zip_path):
            raise ValueError("Provided path is not a valid zip file")
        
        parent_dir = os.path.dirname(zip_path)
        zip_filename = os.path.basename(zip_path)
        
        temp_dir = os.path.join(parent_dir, "_zip_zip")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Define the new zip file path
        new_zip_path = os.path.join(temp_dir, f"{zip_filename}")
        
        # Zip the original zip file
        with zipfile.ZipFile(new_zip_path, 'w', zipfile.ZIP_DEFLATED) as new_zip:
            new_zip.write(zip_path, arcname=zip_filename)
        return new_zip_path


@dataclass
class DvUploaderWrapper(FileUploader):
    """A wrapper that invokes DVUploader to upload files."""

    def __post_init__(self):
        self._dv_uploader_path = self._ensuere_dv_uploader()
    
    def _ensure_dv_uploader():
        return

    def create(self, df):
        return NotImplemented
       
    def replace(self, file_pid, df):
        return NotImplemented

def upload_dataset(
    dv_config,
    parent_id,
    dataset_index,
    action_on_exist="none",
    metadata_only=False,
    publish_type="none",
    output=None,
    direct_upload=False,
):
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
    metadata_only : bool
        If True, files are skipped.
    publish_type : {'none', 'major', 'minor', 'updatecurrent'}
        How the dataset is published:
        * 'none': do not publish.
        * 'major': publish with major version bump.
        * 'minor': publish with minor version bump.
    file_upload_method: {"native", "dvuploader"}
        If provided, the callable is used to upload files.
    """
    api = dv_config.native_api
    # parent info
    resp = api.get_dataverse(parent_id)
    logger.info(f"query parent dataverse:\n{pformat_resp(resp)}")
    parent_meta = resp.json().pop("data")
    logger.debug(f"parent dataverse meta:\n{pformat_yaml(parent_meta)}")
    logger.debug(f"dataset index meta:\n{pformat_yaml(dataset_index['meta'])}")

    # create ds object
    ds = DVDataset()
    ds.set(dataset_index["dataset"])
    ds_json = ds.json()
    logger.info("VALIDATING...")
    logger.info(ds_json)
    assert ds.validate_json()
    logger.info("OK")
    logger.info(f"action_on_exist : {action_on_exist}")
    logger.info(
        f"DATASET INDEX FILES: {dataset_index['files']}, len={len(dataset_index['files'])}"
    )

    file_action = action_on_exist
    if metadata_only:
        file_action = "none"
        
    def _create():
        logger.debug(f"create dataset json:\n{ds_json}")
        resp = api.create_dataset(
            parent_id, ds_json, pid=None, publish=False, auth=True
        )
        logger.info(f"create dataset response:\n{pformat_resp(resp)}")
        if not resp.ok:
            raise ValueError(f"Failed create dataset:\n{pformat_resp(resp)}")
        # set file action to create for newly created datasets
        nonlocal file_action
        file_action = "create"
        # get pid
        return str(resp.json()["data"]["persistentId"])

    if action_on_exist == "create":
        # just create
        pid = _create()
    else:
        # search for existing dataset
        search_kwargs = {
            "q_str": f'title:"{dataset_index["dataset"]["title"]}"',
            "subtree": parent_id,
            "sort": "date",
            "order": "desc",
        }
        results = search_dataverse(dv_config=dv_config, **search_kwargs)
        if not results:
            # not exist, create
            logger.debug(
                f"no dataset found with "
                f"search_kwargs:\n{pformat_yaml(search_kwargs)}"
            )
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
            pid = str(results[0]["global_id"])
            if action_on_exist == "none":
                # nothing need to be done
                logger.debug(
                    f"action_on_exist is none and dataset exists pid={pid},"
                    f" nothing to do."
                )
                # return pid
            elif action_on_exist == "update":
                logger.debug("update dataset with metadata")
                # TODO implement this
                # update dataset with pid
                # raise NotImplementedError()
                # logger.warning("metadata update is not implemented yet, skipped.")
                ds_dict = json.loads(ds_json)
                ds_json_new = json.dumps(ds_dict["datasetVersion"], indent=2)
                logger.debug(f"update dataset metadata json:\n{ds_json_new}")
                url = "{0}/datasets/:persistentId/versions/:draft?persistentId={1}".format(
                    api.base_url_api_native, pid
                )
                resp = api.put_request(url, ds_json_new, auth=True)
                # resp = api.edit_dataset_metadata(pid, ds_json_new, replace=True, auth=True)
                logger.info(f"update dataset metadata response:\n{pformat_resp(resp)}")
                if not resp.ok:
                    raise ValueError(
                        f"Failed update dataset metadata:\n{pformat_resp(resp)}"
                    )
            else:
                pass
                # raise ValueError("invalid action.")

    # if we reach here, we need to handle the datafiles
    logger.info(f"handle datafiles for dataset pid: {pid}; {file_action=}")
    data_files = list()
    # we retrieve the list of data files in the dataset
    # if action is to update.
    if file_action == "update":
        files_remote = get_datafiles(dv_config, dataset_id=pid)
        logger.debug(f"existing files:\n{files_remote}")

    # file uploader
    if direct_upload:
        file_uploader = DVUploaderWrapper(dv_config)
    else:
        file_uploader = FileUploaderNative(dv_config)
   
    for data in dataset_index["files"]:
        # update pid to point to the dataset.
        data["pid"] = pid
        df = DVDatafile()
        df.set(data)
        assert df.validate_json()
        data_files.append(df)
        # upload file if needed
        if file_action == "create":
            file_uploader.create(df) 
        elif file_action == "update":
            # check if the file is in the list of existing files
            if len(files_remote) == 0:
                m = None
            else:
                m = files_remote[files_remote["label"] == df.label]
            if m is not None and len(m) > 0:
                if len(m) > 1:
                    logger.warning(f"multiple files found with label={df.label}")
                # TODO implement checksum comparison to do update?
                # file_pid = m[0]['dataFile']['id']
                # resp = api.replace_datafile(
                #     file_pid, df.filename, df.json(), is_filepid=False)
                # logger.info(
                #     f"update existing datafile:\n{pformat_resp(resp)}")
                logger.warning(f"overwrite existing datafile label={df.label}")
                file_pid = m[0]["dataFile"]["id"]
                file_uploader.replace(file_pid, df)
            else:
                # not exist yet, create
                file_uploader.create(df)
        else:
            logger.debug("no action specified for file uploading, skipped.")
    # finally, publish the dataset if requested
    if publish_type not in ["none"]:
        resp = api.publish_dataset(pid, publish_type)
        logger.info(f"publish dataset pid={pid}: {pformat_resp(resp)}")
    # print out version info
    v = get_versions(dv_config, dataset_id=pid)
    vv = v[
        [
            "id",
            "datasetId",
            "datasetPersistentId",
            "versionNumber",
            "versionMinorNumber",
            "versionState",
            "lastUpdateTime",
        ]
    ]
    logger.info(f"current versions:\n{vv}")

    # generate output index file
    index_out = deepcopy(dataset_index)
    index_out["meta"].update(
        {"dataset": {"pid": pid, "versions": v.to_pandas().to_dict(orient="records")}}
    )
    if output is not None:
        with open(output, "w") as fo:
            yaml.dump(index_out, fo)
        logger.info(f"output yaml written to: {output}")
    return pid
