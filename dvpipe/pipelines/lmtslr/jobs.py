#!/usr/bin/env python

r"""
Dataverse collector flow for LMT SLR TAPs.

The flow takes a work_lmt dir as input, and does roughly the following:

1. Traverse the filesystem tree at parent_path to look for dirs with names
matching re_project_dirname in the config.

2. For each matching project folder:
     1. Check if dvp_{proj_id}.yaml exists in a cache folder.
     2. Create the dvp.yaml file if no cache found.
     3. Call dvpipe.Dataset.from_yaml to create dataset.
     4. Check if dataset exists in dataverse and create or update it.
"""

from dagster import graph
# from dagster import (graph, config_mapping, Permissive)

from .resources import (
    dataset_index_io_manager, project_dir,
    # dvpipe_config
    )
from dvpipe.dagster.resources import DVPIPE_RESOURCES
from .ops import (
    count_items,
    get_project_dirs,
    create_dataset_index_from_project_dir,
    upload_dataset_to_dataverse
    )


_all_graphs = list()  # keep track of all the graphs


def _add_graph(fn):
    """Decorator to set `fn` as graph and add to the `_all_graphs` list."""
    _all_graphs.append(graph(fn))


@_add_graph
def create_lmtslr_project_dataset_indices():
    dataset_indices = get_project_dirs().map(
        create_dataset_index_from_project_dir)
    return count_items(dataset_indices.collect())


@_add_graph
def upload_lmtslr_project_datasets():
    dataset_urls = get_project_dirs().map(
        create_dataset_index_from_project_dir).map(
            upload_dataset_to_dataverse
            )
    return count_items(dataset_urls.collect())


def _make_job(g):

    return g.to_job(
        resource_defs={
            "project_dir": project_dir,
            'dataset_index_io_manager': dataset_index_io_manager,
            **DVPIPE_RESOURCES
            },
        )


jobs = [_make_job(g) for g in _all_graphs]
"""List of jobs defined in this module."""
