from dvpipe.pipelines.metadatablock import MetadataBlock
from dvpipe.pipelines.metadb import MetaDB
import pandas as pd
import json
import dvpipe.utils as utils
import astropy.units as u
import sqlite3
import os

class LmtMetadataBlock(MetadataBlock):
    def __init__(self,dbfile=None,yamlfile=None):
      self._datacsv = utils.aux_file("LMTMetaDatablock.csv")
      self._vocabcsv =  utils.aux_file("LMTControlledVocabulary.csv")
      self._almakeyscsv =  utils.aux_file("alma_to_lmt_keymap.csv")
      self._dbfile = dbfile
      self._yamlfile = yamlfile
      self._db = None
      super().__init__("LMTData",self._datacsv,self._vocabcsv)
      self._map_lmt_to_alma()
      self._version = "1.0.7"

    def _map_lmt_to_alma(self):
        self._lmt_map = dict()
        #TODO trim trailing spaces will will get us intro trouble possibly later
        self._alma_keys =  pd.read_csv(self._almakeyscsv,skipinitialspace=True)
        self._lmt_keys = self._alma_keys[self._alma_keys['LMT Keyword'].notna()]
        tablenames = set(self._alma_keys['Database Table'])
        for name in tablenames:
            kv = self._lmt_keys[(self._lmt_keys['Database Table'] == name)]
            self._lmt_map[name] = dict(zip(kv['LMT Keyword'],kv['ALMA Keyword']))

    def _open_db(self,create=True):
       # True: will create if not exists
        self._db = MetaDB(self._dbfile,create) 
        if self._db._created:
            self._alma_id = 1
        else:
            # Get the highest alma_id in the table and add 1 as each metadata is a new entry
            # This query returns list[tuple], hence the double indices.
            self._alma_id = self._db.query("alma","MAX(id)")[0][0] + 1
        #print("ALMA ID is ",self._alma_id)

    def _write_to_yaml(self):
        if self._yamlfile is None:
            print(f"yamlfile is not set, can't write")
            return
        print(f"Writing to YML file: {self._yamlfile}")
        f = open(self._yamlfile,"w")
        f.write(self.to_yaml())
        f.close()

    def _write_to_db(self):
        if self.dbfile is None:
            print(f"dbfile is not set, can't write")
            return

        print(f"Writing to sqlite file: {self.dbfile}")
        if self._db is None:
            self._open_db()
        
        #loop over the metadata. First do the bands
        for b in self._metadata["band"]:
            insertme= dict()
            df = self._lmt_keys[(self._lmt_keys['Database Table'] == "win")]
            # there must be a quicker way to do this with pure pandas
            for ak in df['ALMA Keyword']:
                x = df.loc[df['ALMA Keyword'] == ak]
                insertme[ak] = b[x['LMT Keyword'].array[0]]
            #print("Attempting to insert: ",insertme)
            insertme["a_id"] = self._alma_id
            self._db.insert_into("win",insertme) 
        
        dolist = self._lmt_keys[(self._lmt_keys['Database Table'] != "win")]['Database Table']
        #print("DOLIST",set(dolist))
        for name in set(dolist):
            insertme = dict()
            for k,v in self._lmt_map[name].items():
                #print(f"{name}.{v}={k}")
                if k in self._metadata:
                    insertme[v] = self._metadata[k]
            #print("I:",insertme,len(insertme),not insertme)
            if insertme: # don't insert empty dict
                self._db.insert_into(name,insertme)
        self._alma_id += 1

    @property
    def dbfile(self):
        return self._dbfile

    @property
    def yamlfile(self):
        return self._yamlfile

    def test(self):
        try:
            self.add_metadata("foobar",12345)
        except KeyError as v:
            print("Caught as expected: ",v)
        print(self.controlledVocabulary)
        print(self._check_controlled("velFrame","Foobar"))
        print(self._check_controlled("velFrame","LSR"))
        print(self._check_controlled("foobar","uhno"))
        try:
            self.add_metadata("velFrame","Foobar")
        except ValueError as v:
            print("Caught as expected: ",v)
        print(self._has_parent("slBand"))
        print(self._has_parent("velFrame"))
        print(self._is_parent("band"))
        print(self.get_children("band"))
        print(self.get_children("targetName"))
        if False:
            print("JSON")
            print(self.to_json())

def example(dbfile=None,yamlfile=None):
    '''Example usage of LmtMetadataBlock'''
    lmtdata = LmtMetadataBlock(dbfile=dbfile,yamlfile=yamlfile)
    lmtdata.add_metadata("projectID","2021-S1-US-3")
    lmtdata.add_metadata("projectTitle","Life, the Universe, and Everything")
    lmtdata.add_metadata("PIName","Marc Pound")
    lmtdata.add_metadata("obsnum","12345") 
#   for multiple obsnums:
    #lmtdata.add_metadata("obsnum","12345,56783,42099") 
    lmtdata.add_metadata("RA",14.01,"degree")
    lmtdata.add_metadata("DEC",-43.210)
    # add a band
    band = dict()
    band["slBand"] = 1
    band["formula"]='CS'
    band["transition"]='2-1'
    band["frequencyCenter"] = 97981*u.Unit("MHz")
    band["velocityCenter"] = 300.0 #km/s
    band["bandwidth"] = 2.5
    band["beam"] = u.Quantity(20.0,"arcsec")
    band["lineSens"] = 0.072
    band["qaGrade"] = "A+++"
    band["nchan"] = 1024
    lmtdata.add_metadata("band",band)
    # add a second band
    band["slBand"] = 2
    band["formula"]='CO'
    band["transition"]='1-0'
#   for multiple lines:
    band["frequencyCenter"] = u.Quantity(115.2712,"GHz")
    band["bandwidth"] = 2.5 #GHz
    band["beam"] = (97.981/115.2712)*20.0/3600.0
    band["lineSens"] = 123*u.Unit("mK")
    band["qaGrade"] = "B-"
    band["velocityCenter"] = -25.0 #km/s
    band["nchan"] = 2048
    lmtdata.add_metadata("band",band)

    lmtdata.add_metadata("obsDate",utils.now())
    lmtdata.add_metadata("intTime",30.0,"minute")
    lmtdata.add_metadata("velocity",321.0,u.Unit("m/s"))
    lmtdata.add_metadata("velDef","RADIO")
    lmtdata.add_metadata("velFrame","LSR")
    lmtdata.add_metadata("velType","FREQUENCY")
    lmtdata.add_metadata("z",0.001071)
    lmtdata.add_metadata("observatory","LMT")
    lmtdata.add_metadata("LMTInstrument","SEQUOIA")
    lmtdata.add_metadata("targetName","NGC 5948")
    lmtdata.add_metadata("calibrationStatus","UNCALIBRATED")# or CALIBRATED
    # YAML output
    #print(lmtdata.to_yaml())
    return lmtdata


class CitationMetadataBlock(MetadataBlock):
    def __init__(self):
      self._datacsv = utils.aux_file("CitationMetaDatablock.csv")
      self._vocabcsv =  utils.aux_file("CitationControlledVocabulary.csv")
      super().__init__("CitationData",self._datacsv,self._vocabcsv)
      self._version = "Dataverse 5.12.1"
