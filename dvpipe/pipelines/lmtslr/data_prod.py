#!/usr/bin/env python

from astropy.table import Table


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

    @classmethod
    def from_project_dir(cls, project_dir):
        """Load data product from `project_dir`."""
        meta = dict()
        data_items = list()

        meta.update({
            'project_dir': project_dir,
            'project_id': project_dir.name,
            })

        # add items to data items
        # data_items.append()

        index_table = Table(rows=data_items)
        index_table.meta.update(**meta)
        return cls(index_table=index_table)

    def make_dataverse_dataset_index(self):
        """Create dataverse dataset index from this data product."""
        # TODO populate this
        return {
            'meta': self.meta
            }
