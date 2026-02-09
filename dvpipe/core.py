from loguru import logger
from pydantic import BaseModel, BaseSettings
from pydantic_yaml import YamlModelMixin
from typing import Optional, Dict

from pyDataverse.api import DataAccessApi, NativeApi, MetricsApi, SearchApi


class DataverseConfig(BaseModel):
    """The config class for the dataverse instance."""
    api_token: str
    base_url: str

    def get_api(self, type):
        """Return the pyDataverse.Api instance of `type`.

        Parameters
        ----------
        type : {'data_access', 'native', 'metrics', 'search'}
            The type of API.
        """
        dispatch_type = {
            'data_access': DataAccessApi,
            'native': NativeApi,
            'metrics': MetricsApi,
            'search': SearchApi
            }
        api_cls = dispatch_type.get(type, None)
        if api_cls is None:
            raise ValueError(f"Invalid api type {type}")
        api = api_cls(base_url=self.base_url, api_token=self.api_token)
        logger.debug(f"created dataverse api instance: {api}")
        return api

    @property
    def data_access_api(self):
        """The data access api instance."""
        return self.get_api('data_access')

    @property
    def native_api(self):
        """The native api instance."""
        return self.get_api('native')

    @property
    def metrics_api(self):
        """The metrics api instance."""
        return self.get_api('metrics')

    @property
    def search_api(self):
        """The search api instance."""
        return self.get_api('search')

    class Config:
        pass


class DVPConfig(YamlModelMixin, BaseSettings):
    """The config class for dvpipe.

    The entries can be specified via environment variables.
    """

    dataverse: DataverseConfig
    dagster: Optional[Dict]

    class Config:
        env_prefix = 'DVPIPE_'  # defaults to no prefix, i.e. ""
        env_nested_delimiter = '__'
        env_file = '.env'
        env_file_encoding = 'utf-8'
        allow_mutation = False
