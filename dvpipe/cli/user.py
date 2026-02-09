from loguru import logger
import click
import pandas as pd

from ..utils import pformat_yaml, pformat_resp


@click.group(
    'user',
    invoke_without_command=True,
    )
@click.pass_obj
def cmd_user(ctxobj, ):
    """The CLI for user management."""
    api = ctxobj.dvpipe.dataverse.native_api
    url = f"{api.base_url_api_native}/users/:me"
    resp = api.get_request(url, auth=True)
    logger.debug(f'query current user:\n{pformat_resp(resp)}')
    data = resp.json().pop("data")
    print(pformat_yaml(data))


@cmd_user.command('list')
@click.pass_obj
def cmd_user_list(ctxobj):
    """List all users."""
    api = ctxobj.dvpipe.dataverse.native_api
    url = f"{api.base_url_api_native}/admin/list-users"
    resp = api.get_request(url, auth=True, params={
        'itemsPerPage': 1000
        })
    data = resp.json().pop('data')
    users = data.pop('users')
    logger.debug(f'query all users:\n{pformat_resp(resp)}')
    logger.debug(f'metadata:\n{data}')
    df = pd.DataFrame.from_records(users)
    print(df)
