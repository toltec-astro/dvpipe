#!/usr/bin/env python

from typing import List, Any
from dagster import op, Out, Output, MetadataEntry, MetadataValue


@op(
    out=Out(int),
    description="Count number of items.",
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
