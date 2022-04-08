#!/usr/bin/env python

import re
from pathlib import Path
from typing import List, Any

from dagster import (
    Out, Output,
    DynamicOut, DynamicOutput, MetadataValue, MetadataEntry,
    op)


def _make_mapping_key(p: Path):

    return p.name.replace('-', '_')


@op(
    required_resource_keys={"project_dir"},
    out=DynamicOut(str),
    description="Get project directories.",
)
def get_project_dirs(context):
    parent_path = Path(context.resources.project_dir['parent_path'])
    re_project_dirname = context.resources.project_dir['re_project_dirname']
    for p in parent_path.iterdir():
        if p.is_dir() and re.match(re_project_dirname, p.name):
            yield DynamicOutput(
                value=p.as_posix(),
                mapping_key=_make_mapping_key(p))


@op(
    out=Out(
        dict,
        io_manager_key="dataset_index_io_manager",
        ),
    description="Create dataset index file from project directory.",
    )
def create_dataset_index_from_project_dir(context, project_dir: str) -> dict:
    project_dir = Path(project_dir)
    meta = {
        'project_id': project_dir.name,
        }
    context.log.info(f"metadata: {meta}")
    yield Output(
        {
            'meta': meta,
            },
        metadata_entries=[
            MetadataEntry(
                value=MetadataValue.text(meta['project_id']),
                label='project_id'
                ),
            MetadataEntry(
                value=MetadataValue.path(project_dir),
                label='project_dir'
                ),
            ],
        )


@op(
    required_resource_keys={"dataverse"},
    out=Out(str),
    description="Get project directories.",
)
def upload_dataset_to_dataverse(context, dataset_index: dict):
    dataverse = context.resources.dataverse
    api = dataverse.api
    resp = api.get_info_version()
    context.log.info(f"api version query: {resp.json()}")
    meta = dataset_index['meta']
    # TODO
    # implement the actual flow
    url = f'dataset_url_{meta["project_id"]}'
    yield Output(
        url,
        metadata_entries=[
            MetadataEntry(
                value=MetadataValue.text(meta['project_id']),
                label='project_id'
                ),
            MetadataEntry(
                value=MetadataValue.url(url),
                label='dataset_url'
                )
            ]
        )


@op(
    out=Out(int),
    description="Count datasets processed.",
    )
def count_items(items: List[Any]) -> int:
    n = len(items)
    yield Output(
        n,
        metadata_entries=[
            MetadataEntry(
                value=MetadataValue.int(n),
                label='n_items'
                ),
            ]
        )
