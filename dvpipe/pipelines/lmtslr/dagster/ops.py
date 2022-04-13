#!/usr/bin/env python

import re
from pathlib import Path

from dagster import (
    Out, Output,
    DynamicOut, DynamicOutput, MetadataValue, MetadataEntry,
    op, Field, Enum, EnumValue)

from dvpipe.dataverse import upload_dataset
from ..data_prod import LmtslrDataProd


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
    dp = LmtslrDataProd.from_project_dir(Path(project_dir))
    meta = dp.meta
    context.log.info(f"metadata: {meta}")
    yield Output(
        dp.make_dataverse_dataset_index(),
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
    required_resource_keys={"dataverse_config"},
    config_schema={
        'parent_id': str,
        'action_on_exist': Field(
            Enum(
                'ActionOnExist',
                [
                    EnumValue('none'),
                    EnumValue('create'),
                    EnumValue('update'),
                    ]
                ),
            default_value='none'
            ),
        'publish_type': Field(
            Enum(
                'PublishType',
                [
                    EnumValue('none'),
                    EnumValue('major'),
                    EnumValue('minor'),
                    ]
                ),
            default_value='none'
            ),
        },
    out=Out(str),
    description="Upload dataset to dataverse.",
)
def upload_dataset_to_dataverse(context, dataset_index: dict):

    dataverse_config = context.resources.dataverse_config
    meta = dataset_index['meta']
    dataset_url = upload_dataset(
        dataverse_config,
        dataset_index=dataset_index,
        **context.op_config)
    yield Output(
        dataset_url,
        metadata_entries=[
            MetadataEntry(
                value=MetadataValue.text(meta['project_id']),
                label='project_id'
                ),
            MetadataEntry(
                value=MetadataValue.url(dataset_url),
                label='dataset_url'
                )
            ]
        )
