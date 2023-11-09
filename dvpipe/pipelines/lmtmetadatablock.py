from dvpipe.pipelines.metadatablock import MetadataBlock
from dvpipe.pipelines.metadb import MetaDB
import pandas as pd
import json
import dvpipe.utils as utils
import astropy.units as u
import sqlite3
import numpy as npy
import os


class LmtMetadataBlock(MetadataBlock):
    def __init__(self, dbfile=None, yamlfile=None, load_data=False):
        self._datacsv = utils.aux_file("LMTMetaDatablock.csv")
        self._vocabcsv = utils.aux_file("LMTControlledVocabulary.csv")
        self._almakeyscsv = utils.aux_file("alma_to_lmt_keymap.csv")
        self._dbfile = dbfile
        self._yamlfile = yamlfile
        self._db = None
        super().__init__("LMTData", self._datacsv, self._vocabcsv)
        self._map_lmt_to_alma()
        self._version = "1.2.1"
        if load_data and yamlfile is not None:
            self.load_from_yaml(yamlfile)

    def _map_lmt_to_alma(self):
        self._lmt_map = dict()
        # TODO trim trailing spaces will will get us into trouble possibly later
        self._alma_keys = pd.read_csv(self._almakeyscsv, skipinitialspace=True)
        self._lmt_keys = self._alma_keys[self._alma_keys["LMT Keyword"].notna()]
        tablenames = set(self._alma_keys["Database Table"])
        for name in tablenames:
            kv = self._lmt_keys[(self._lmt_keys["Database Table"] == name)]
            self._lmt_map[name] = dict(zip(kv["LMT Keyword"], kv["ALMA Keyword"]))

    def _open_db(self, create=True):
        # True: will create if not exists
        self._db = MetaDB(self._dbfile, create)
        if self._db._created:
            self._alma_id = 1
        else:
            # Get the highest alma_id in the table and add 1 as each metadata is a new entry
            # This query returns list[tuple], hence the double indices.
            self._alma_id = self._db.query("alma", "MAX(id)")[0][0] + 1
        # print("ALMA ID is ",self._alma_id)

    def _write_to_yaml(self):
        if self._yamlfile is None:
            print(f"yamlfile is not set, can't write")
            return
        print(f"Writing to YML file: {self._yamlfile}")
        f = open(self._yamlfile, "w")
        f.write(self.to_yaml())
        f.close()

    def _write_to_db(self):
        if self.dbfile is None:
            print(f"dbfile is not set, can't write")
            return

        print(f"Writing to sqlite file: {self.dbfile}")
        _inserted = dict()
        if self._db is None:
            self._open_db()
            # write version info to header
            h = dict()
            h["version"] = f"LMT Metadata Version {self._version}"
            self._db.insert_into("header", h)

        # loop over the metadata. First do the bands
        for b in self._metadata["band"]:
            insertme = dict()
            df = self._lmt_keys[(self._lmt_keys["Database Table"] == "win")]
            # there must be a quicker way to do this with pure pandas
            for ak in df["ALMA Keyword"]:
                x = df.loc[df["ALMA Keyword"] == ak]
                if x["LMT Keyword"].array[0] in b:
                    insertme[ak] = b[x["LMT Keyword"].array[0]]
            # print("Attempting to insert: band.", insertme)
            insertme["a_id"] = self._alma_id
            self._db.insert_into("win", insertme)
            _inserted.update(insertme)

        # don't need to do the rest of the "win" table because they
        # aren't relevant to LMT

        # then do obsInfo
        for b in self._metadata["obsInfo"]:
            insertme = dict()
            df = self._lmt_keys[(self._lmt_keys["Database Table"] == "alma")]
            # there must be a quicker way to do this with pure pandas
            for ak in df["ALMA Keyword"]:
                x = df.loc[df["ALMA Keyword"] == ak]
                # print("doing  ",x['LMT Keyword'].array[0])
                if x["LMT Keyword"].array[0] in b:
                    insertme[ak] = b[x["LMT Keyword"].array[0]]
                else:
                    insertme[ak] = self._metadata[x["LMT Keyword"].array[0]]
            # print("Attempting to insert obsInfo: ", insertme)
            self._db.insert_into("alma", insertme)
            _inserted.update(insertme)

        # dolist = self._lmt_keys[(self._lmt_keys['Database Table'] != "win")]['Database Table']
        dolist = self._lmt_keys["Database Table"]
        # print("DOLIST", set(dolist))
        for name in set(dolist):
            # Q: we already did 'win' too should we skip that??
            if name == "alma":
                continue  # we already did alma with obsInfo
            insertme = dict()
            for k, v in self._lmt_map[name].items():
                # print(f"{name}.{v}={k}")
                if k in self._metadata and k not in _inserted:
                    insertme[v] = self._metadata[k]
            # print("I:", insertme, len(insertme), not insertme)
            if insertme:  # don't insert empty dict
                self._db.insert_into(name, insertme)
        self._alma_id += 1

    @property
    def dbfile(self):
        return self._dbfile

    @property
    def yamlfile(self):
        return self._yamlfile

    @classmethod
    def from_yaml(cls, yamlfile):
        return cls(yamlfile=yamlfile, load_data=True)

    def to_dataverse_dict(self):
        """output in the particular upload format that dataverse wants
        See e.g. http://lmtdv1.astro.umass.edu/api/datasets/2/versions/1/metadata
        """
        md = self._metadata
        df = self._datasetFields
        fields = []
        fdict = {"fields": fields}

        # yes iterrows is frowned upon for performance, but we don't have many rows
        for index, row in df.iterrows():
            d = dict()
            p = row["name"]
            am = row["allowmultiples"]
            if pd.isnull(row["parent"]):
                if self._is_parent(p):
                    d["typeName"] = p
                    d["multiple"] = am
                    d["typeClass"] = "compound"
                    d["value"] = []
                    children = self.get_children(row["name"])
                    nparent = len(md[p])
                    for np in range(nparent):
                        # for each item, create a dict containing
                        # all chilren
                        parent_dict = dict()
                        for c in children:
                            child_dict = dict()
                            r = df[df["name"] == c]
                            am = r["allowmultiples"].values[0]
                            fieldType = r["fieldType"].values[0]
                            if self.is_controlled(c):
                                tc = "controlledVocabulary"
                            else:
                                tc = "primitive"
                            child_dict["typeName"] = c
                            child_dict["typeClass"] = tc
                            child_dict["multiple"] = am
                            rawvalue = md[p][np][c]
                            # handle boolean types
                            if isinstance(rawvalue, npy.bool_):
                                child_dict["value"] = bool(rawvalue)
                            elif isinstance(md[p][np][c], str):
                                child_dict["value"] = rawvalue
                            else:
                                if fieldType == "int":
                                    rawvalue = int(rawvalue)
                                child_dict["value"] = str(rawvalue)
                            # print("appending child ",child_dict)
                            parent_dict[c] = child_dict
                        d["value"].append(parent_dict)
                else:
                    if self.is_controlled(p):
                        tc = "controlledVocabulary"
                    else:
                        tc = "primitive"
                    d["typeName"] = p
                    d["typeClass"] = tc
                    d["multiple"] = am
                    fieldType = row["fieldType"]
                    rawvalue = md[p]
                    if isinstance(rawvalue, npy.bool_):
                        d["value"] = bool(rawvalue)
                    elif isinstance(rawvalue, str):
                        d["value"] = rawvalue
                    # TODO fix this
                    # elif p in ["calibrationLevel"]:
                    #    d["value"] = str(int(md[p]))
                    else:
                        if fieldType == "int":
                            rawvalue = int(rawvalue)
                        d["value"] = str(rawvalue)
                fields.append(d)
        dvdict = {self.name: fdict}
        return dvdict

    def test(self):
        try:
            self.add_metadata("foobar", 12345)
        except KeyError as v:
            print("Caught as expected: ", v)
        print(self.controlledVocabulary)
        print(self._check_controlled("velFrame", "Foobar"))
        print(self._check_controlled("velFrame", "LSR"))
        print(self._check_controlled("foobar", "uhno"))
        try:
            self.add_metadata("velFrame", "Foobar")
        except ValueError as v:
            print("Caught as expected: ", v)
        print(self._has_parent("slBand"))
        print(self._has_parent("velFrame"))
        print(self._is_parent("band"))
        print(self.get_children("band"))
        print(self.get_children("targetName"))
        if False:
            print("JSON")
            print(self.to_json())


class CitationMetadataBlock(MetadataBlock):
    def __init__(self):
        self._datacsv = utils.aux_file("CitationMetaDatablock.csv")
        self._vocabcsv = utils.aux_file("CitationControlledVocabulary.csv")
        super().__init__("CitationData", self._datacsv, self._vocabcsv)
        self._version = "Dataverse 5.12.1"
