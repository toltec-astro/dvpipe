#!/usr/bin/env python


from dagster import repository
from .jobs import jobs


@repository
def lmtslr_pipeline_repository():
    return jobs
