import re
from astropy.table import Table
from pathlib import Path
import datetime
from ...dataverse import DVDataset, DVDatafile
from ..lmtmetadatablock import LmtMetadataBlock


class LmtslrDataProd(object):
    """A class represent LMT SLR data product."""

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

    #_glob_lmtmetadata_yaml = "lmtmetadata.yaml"
    _glob_lmtmetadata_yaml = "[0-9]*_lmtmetadata.yaml"
    _glob_data_product_types = {
        # Tar versions
        # Science Ready Data Products
        "SRDP": "[0-9]*_SRDP.tar",
        # SDFITS file
        "SDFITS": "[0-9]*_SDFITS.tar",
        # FITS image
        "FITS": "[0-9]*_FITS.tar",
        # Zip versions, e.g. whatever.zip or whatever_partial.z01,z02,z03 etc
        "SRDP": "[0-9]*_SRDP*.z*",
        "SDFITS": "[0-9]*_SDFITS*.z*",
        "FITS": "[0-9]*_FITS*.z*",
    }

    @classmethod
    def _get_project_meta(cls, project_dir):
        # collect metadata from project dir
        meta = {
            "project_dir": project_dir,
            "project_id": project_dir.name,
            "archive_rootpath": Path(project_dir.name),
        }
        return meta

    @classmethod
    def _get_data_prod_data_items(cls, data_prod_dir, archive_rootpath=None):
        # collect data items from data_prod_dir
        # collect obsnums by parsing the tap tar names
        data_items = []
        if archive_rootpath is None:
            archive_rootpath = Path("unnamed_project")
        # loop over all the data prodcut types
        for key, value in cls._glob_data_product_types.items():
            for tar_path in data_prod_dir.glob(value):
                meta = {
                    "name": tar_path.name,
                    "data_prod_type": key,
                    "archive_path": archive_rootpath.joinpath(data_prod_dir),
                }
                data_items.append({"meta": meta, "filepath": tar_path})
        return data_items

    @classmethod
    def _get_data_prod_meta(cls, data_prod_dir):
        # collect lmtdata metadata block
        lmtmetadata_path = list(data_prod_dir.glob(cls._glob_lmtmetadata_yaml))
        if not len(lmtmetadata_path) == 1:
            raise ValueError(f"No LMTData metadata yaml found in {data_prod_dir}")
        lmtmetadata_path = lmtmetadata_path[0]
        lmtmetadata_block = LmtMetadataBlock.from_yaml(lmtmetadata_path)
        return {
            "name": data_prod_dir.name,
            "path": data_prod_dir,
            "metadata_blocks": [
                lmtmetadata_block,
            ],
            "date_public": lmtmetadata_block.metadata["publicDate"],
        }

    @classmethod
    def from_project_dir(cls, project_dir):
        """Load data products from `project_dir`.

        This will look for subdirectories in the `project_dir` and return
        a list of `LmtSlrDataProd` items.
        """
        project_meta = cls._get_project_meta(project_dir)
        data_prod_list = []
        for data_prod_dir in project_dir.iterdir():
            data_items = cls._get_data_prod_data_items(
                data_prod_dir, archive_rootpath=project_meta["archive_rootpath"]
            )
            if not data_items:
                continue
            data_prod_meta = cls._get_data_prod_meta(data_prod_dir)
            n_cols = len(data_items[0])
            index_table = Table(rows=data_items, dtype=[object] * n_cols)
            index_table.meta["project_meta"] = project_meta
            index_table.meta["data_prod_meta"] = data_prod_meta
            data_prod_list.append(cls(index_table=index_table))
        return data_prod_list

    def make_dataverse_dataset_index(self):
        """Create dataverse dataset index from this data product."""
        meta = self.meta
        # TODO generate these info
        project_meta = meta["project_meta"]
        data_prod_meta = meta["data_prod_meta"]
        project_id = project_meta["project_id"]
        data_prod_name = data_prod_meta["name"]
        data = {
            "title": f"{project_id}_{data_prod_name}",
            "dsDescription": [
                {"dsDescriptionValue": (f"Data product for project {project_id}")}
            ],
            "author": [
                {
                    "authorName": "LMT",
                    "authorAffiliation": "Large Millimeter Telescope",
                    "authorEmail": "dp@lmtgtm.org",
                },
            ],
            "datasetContact": [
                {
                    "datasetContactName": "LMT",
                    "datasetContactAffiliation": "Large Millimeter Telescope",
                    "datasetContactEmail": "dp@lmtgtm.org",
                },
            ],
            "subject": ["Astronomy and Astrophysics"],
        }
        # generate metadatablock fields data
        metadata_blocks = {}
        for mdb in data_prod_meta["metadata_blocks"]:
            metadata_blocks.update(mdb.to_dataverse_dataset_fields())
        data["metadata_blocks"] = metadata_blocks
        # print(metadata_blocks)
        # create the dataverse dataset
        ds = DVDataset()
        ds.set(data)
        assert ds.validate_json()
        # handle data items
        date_current = datetime.datetime.now()
        date_public = datetime.datetime.fromisoformat(data_prod_meta["date_public"])

        file_is_restricted = date_current < date_public

        datafiles = list()
        for data_item in self.index_table:
            meta = data_item["meta"]
            description = (
                f'{meta["data_prod_type"]} data product of ' f"{data_prod_name}"
            )
            data = {
                "description": description,
                "categories": ["Data"],
                "restrict": file_is_restricted,
                "label": meta["name"],
                "directoryLabel": meta["archive_path"].parent.as_posix(),
                "filename": data_item["filepath"].as_posix(),
                "pid": "not_yet_set",
            }
            df = DVDatafile()
            df.set(data)
            assert df.validate_json()
            datafiles.append(df)
        # dump as dataset_index file
        dataset_index = {
            "meta": {"project_meta": project_meta},
            "dataset": ds.get(),
            "files": [df.get() for df in datafiles],
        }
        return dataset_index
