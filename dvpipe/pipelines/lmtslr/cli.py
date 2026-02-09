import click
from loguru import logger
from pathlib import Path
from ...utils import pformat_yaml, yaml


@click.group(
    name='lmtslr',
    )
@click.pass_context
def cmd_lmtslr(ctx):
    pass


@cmd_lmtslr.command('create_index')
@click.option(
    "--project_dir", '-d',
    required=True,
    metavar='DIR',
    help='Path to project data product directory.'
    )
@click.option(
    '--output_dir', '-o',
    default='.',
    type=click.Path(
        exists=True,
        file_okay=False, dir_okay=True,
        readable=True, writable=True
        ),
    metavar='DIR',
    help='Path to directory to store created dataset_index'
    )
@click.pass_obj
def cmd_create_dataset_index(ctxobj, project_dir, output_dir):
    from ..lmtslr.data_prod import LmtslrDataProd
    dp_list = LmtslrDataProd.from_project_dir(Path(project_dir))
    logger.info(f"found {len(dp_list)} data products:\n")
    for dp in dp_list:
        meta = dp.meta
        logger.info(f"meta:\n{pformat_yaml(meta)}\n")
        dataset_index = dp.make_dataverse_dataset_index()
        output = yaml.dump(dataset_index)
        print(output)
        project_id = meta['project_meta']['project_id']
        data_prod_name = meta['data_prod_meta']['name']
        output_name = f"{project_id}_{data_prod_name}.yaml"
        output_path = Path(output_dir).joinpath(output_name)
        with open(output_path, 'w') as fo:
            fo.write(output)
        logger.info(f"dataset index write to: {output_path}")
