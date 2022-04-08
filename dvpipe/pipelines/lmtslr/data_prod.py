#!/usr/bin/env python

import re
from astropy.table import Table
from pyDataverse.models import Dataset as DVDataset
from pyDataverse.models import Datafile as DVDatafile


class LmtslrDataProd(object):
    """A class represent LMT SLR data product.
    """
    # TODO
    # integrate with tolteca dpdb framework

    def __init__(self, index_table):
        self._index_table = index_table

    @property
    def meta(self):
        return dict(self._index_table.meta)

    @property
    def index_table(self):
        return self._index_table

    _tap_dirpath = 'TAP'
    _glob_tap_tar = '[0-9]*_TAP.tar'

    @classmethod
    def _get_project_meta(cls, project_dir):
        # collect metadata from project dir
        meta = {
            'project_dir': project_dir,
            'project_id': project_dir.name,
            }
        return meta

    @classmethod
    def _get_project_tap_data_items(cls, project_dir):
        # collect data items from project_dir
        tap_dir = project_dir.joinpath(cls._tap_dirpath)
        # collect obsnums by parsing the tap tar names
        re_tapname = r'(?P<obsnum_start>\d+)(?:_(?P<obsnum_end>\d+))?_TAP.tar'
        data_items = list()
        for tap_tar_path in tap_dir.glob(cls._glob_tap_tar):
            meta = dict()
            d = re.match(re_tapname, tap_tar_path.name).groupdict()
            meta.update(d)
            meta['obsnum'] = tap_tar_path.stem
            meta['name'] = tap_tar_path.stem
            meta['data_prod_type'] = 'TAP'
            meta['archive_path'] = tap_tar_path.relative_to(
                project_dir).as_posix()
            data_items.append({
                'meta': meta,
                'filepath': tap_tar_path.as_posix()
                })
        return data_items

    @classmethod
    def from_project_dir(cls, project_dir):
        """Load data product from `project_dir`."""
        meta = cls._get_project_meta(project_dir)
        data_items = cls._get_project_tap_data_items(project_dir)
        n_cols = len(data_items[0])
        index_table = Table(rows=data_items, dtype=[object] * n_cols)
        index_table.meta.update(**meta)
        return cls(index_table=index_table)

    def make_dataverse_dataset_index(self):
        """Create dataverse dataset index from this data product."""
        meta = self.meta
        # TODO generate these info
        data = {
            'title': meta['project_id'],
            'dsDescription': [{
                'dsDescriptionValue': (
                    f'Data project for project {meta["project_id"]}')
                }],
            'author': [
                {
                    'authorName': 'LMT',
                    'authorAffiliation': 'Large Millimeter Telescope',
                    },
                ],
            'datasetContact': [
                {
                    'datasetContactName': 'LMT',
                    'datasetContactAffiliation': 'Large Millimeter Telescope'
                    },
                ],
            'subject': ['Astronomy and Astrophysics'],
            }
        ds = DVDataset()
        ds.set(data)
        assert ds.validate_json()
        # handle data items
        datafiles = list()
        for data_item in self.index_table:
            meta = data_item['meta']
            description = (
                f'{meta["data_prod_type"]} data product of '
                f'obsnum {meta["obsnum"]}'
                )
            data = {
                "description": description,
                "categories": ['Data'],
                "restrict": True,
                "label": meta['name'],
                "directoryLabel": meta['archive_path'],
                "filename": data_item['filepath'],
                'pid': 'not_yet_set'
                }
            df = DVDatafile()
            df.set(data)
            assert df.validate_json()
            datafiles.append(df)
        # dump as dataset_index file
        dataset_index = {
            'meta': self.meta,
            'dataset': ds.get(),
            'files': [df.get() for df in datafiles]
            }
        return dataset_index
