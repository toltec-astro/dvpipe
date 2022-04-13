#!/usr/bin/env python

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
