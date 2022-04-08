#!/usr/bin/env python

from loguru import logger


def upload_dataset(dataset_index, dataverse):
    # TODO implement this
    api = dataverse.api
    resp = api.get_info_version()
    logger.info(f"api version query: {resp.json()}")

    meta = dataset_index['meta']
    dataset_url = f'dataset_url_{meta["project_id"]}'
    return dataset_url
