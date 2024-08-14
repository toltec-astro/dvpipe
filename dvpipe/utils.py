#!/usr/bin/env python

import datetime
import os.path
import warnings
from pathlib import Path

import pyaml
import urllib.parse as urlparse
from urllib.parse import urlencode
from pathlib import PosixPath

from astropy.io.misc import yaml


__all__ = ['yaml', 'pformat_yaml', 'pformat_resp']


def pformat_yaml(obj):
    """Return pretty-formatted YAML string representation for `obj`."""
    return f"\n{pyaml.dump(obj)}"

def pformat_resp(resp):
    # TODO fix this. maybe in our own fork of pydataverse?
    # the url composed by pydatavese contains the api token as params.
    # we split that so it only shows the endpoint
    url = resp.url
    # https://stackoverflow.com/a/2506477/1824372
    url_parts = list(urlparse.urlparse(url))
    query = dict(urlparse.parse_qsl(url_parts[4]))
    for k in ['key', 'User-Agent']:
        query.pop(k)
    url_parts[4] = urlencode(query)
    url = urlparse.urlunparse(url_parts)
    result = {
        'url': url,
        'status_code': resp.status_code,
        'reason': resp.reason,
        }
    if not resp.ok:
        result['content'] = resp.json()
    return pformat_yaml(result)


pyaml.add_representer(None, lambda s, d: s.represent_str(str(d)))


def _path_representer(dumper, p):
    return dumper.represent_str(p.as_posix())


yaml.AstropyDumper.add_representer(PosixPath, _path_representer)

def now():
    """
    :returns: a string representing the current date and time in ISO format
    """
    return datetime.datetime.now().isoformat()


# stolen from pdrtpy/pdrutils.py
#@TODO  use setup.py and pkg_resources to do this properly
def root_dir():
    """Project root directory, including trailing slash

    :rtype: str
    """
    return str(root_path())+'/'

def root_path():
    """Project root directory as path

    :rtype: :py:mod:`Path`
    """
    return Path(__file__).parent

def aux_dir():
    """Project auxillary tables and files directory, including trailing slash

    :rtype: str
    """
    return os.path.join(root_dir(),'aux/')

def aux_file(filename):
    """Return fully qualified path of the auxillary file.

    :param filename: input file name
    :type filename: str
    :rtype: str
    """
    return aux_dir()+filename

