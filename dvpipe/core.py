#!/usr/bin/env python

from loguru import logger
from pydantic import BaseModel, BaseSettings
from pydantic_yaml import YamlModelMixin

from pyDataverse.api import NativeApi
from functools import cached_property


class DataverseConfig(BaseModel):
    """The config class for the dataverse instance."""
    api_token: str
    base_url: str

    @cached_property
    def api(self):
        api = NativeApi(self.base_url, self.api_token)
        logger.debug(f"created dataverse api instance: {api}")
        return api

    class Config:
        keep_untouched = (cached_property, )


class DVPConfig(YamlModelMixin, BaseSettings):
    """The config class for dvpipe.

    The entries can be specified via environment variables.
    """

    dataverse: DataverseConfig

    class Config:
        env_prefix = 'DVPIPE_'  # defaults to no prefix, i.e. ""
        env_nested_delimiter = '__'
        env_file = '.env'
        env_file_encoding = 'utf-8'
        allow_mutation = False
