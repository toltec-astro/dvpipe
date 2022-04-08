#!/usr/bin/env python


from pathlib import Path
from dagster import (
    make_values_resource, Field,
    AssetKey, IOManager, MetadataEntry, MetadataValue,
    OutputContext,
    io_manager, DagsterEventType
    )
from astropy.io.misc import yaml


project_dir = make_values_resource(
    parent_path=Field(
        str,
        description='The parent path of project directories.'),
    re_project_dirname=Field(
        str,
        default_value=r'\d{4}-S\d-(US|MX|UM)-\d+',
        description='The regex of project directory names.'
        )
    )


class DatasetIndexIOManager(IOManager):
    """
    This IOManager stores dataset index files.
    """

    def __init__(self, rootpath):
        self._rootpath = Path(rootpath)

    @staticmethod
    def _search_meta_entry_by_label(context, label):
        all_output_logs = context.step_context.instance.all_logs(
            context.run_id, of_type=DagsterEventType.STEP_OUTPUT
            )
        step_output_log = [
            log for log in all_output_logs
            if log.step_key == context.step_key
            ][0]
        metadata = (
            step_output_log.dagster_event.event_specific_data.metadata_entries
            )
        entry = next(
            iter([e for e in metadata if e.label == label]), None
            )
        if entry:
            return entry.entry_data.text
        return None

    def _make_path(self, context: OutputContext):
        project_id = self._search_meta_entry_by_label(context, 'project_id')
        path = self._rootpath.joinpath(f'{project_id}.yaml')
        if not path.parent.exists():
            path.parent.mkdir()
        return path

    def handle_output(
            self, context: OutputContext,
            dataset_index: dict
            ):
        path = self._make_path(context)
        with open(path, 'w') as fo:
            yaml.dump(dataset_index, fo)
        yield MetadataEntry(
            value=MetadataValue.path(path), label="dataset_index_path")

    def load_input(self, context) -> dict:
        path = self._make_path(context.upstream_output)
        with open(path, 'r') as fo:
            dataset_index = yaml.load(fo)
        context.log.info(
            f"loaded datset_index meta from {path}:\n{dataset_index['meta']}")
        return dataset_index

    def get_output_asset_key(self, context: OutputContext):
        path = self._make_path(context)
        return AssetKey([path.parent.as_posix(), path.name])


@io_manager(
    config_schema={
        "rootpath": Field(
            str,
            description='The directory to store dataset index files.',
            is_required=True)
        },
    )
def dataset_index_io_manager(init_context):
    return DatasetIndexIOManager(
        rootpath=init_context.resource_config['rootpath']
        )
