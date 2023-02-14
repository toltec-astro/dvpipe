from dvpipe.pipelines.metadatablock import MetadataBlock
from dvpipe.pipelines.metadb import MetaDB
import pandas as pd
import json
import dvpipe.utils as utils
import astropy.units as u
import sqlite3
import os

class LmtMetadataBlock(MetadataBlock):
    def __init__(self):
      self._datacsv = utils.aux_file("LMTMetaDatablock.csv")
      self._vocabcsv =  utils.aux_file("LMTControlledVocabulary.csv")
      self._almakeyscsv =  utils.aux_file("alma_to_lmt_keymap.csv")
      super().__init__("LMTData",self._datacsv,self._vocabcsv)
      self._map_lmt_to_alma()
      self._version = "1.0.5"

    def _map_lmt_to_alma(self):
        self._lmt_map = dict()
        alma_keys =  pd.read_csv(self._almakeyscsv)
        self._lmt_keys = alma_keys[alma_keys['LMT Keyword'].notna()]
        tablenames = set(alma_keys['Database Table'])
        for name in tablenames:
            kv = self._lmt_keys[(self._lmt_keys['Database Table'] == name)]
            self._lmt_map[name] = dict(zip(kv['LMT Keyword'],kv['ALMA Keyword']))
    def _write_to_db(self,file):
        if False:
            if not os.path.exists(file):
                # create the database file
                db = MetaDB(file)
        foo = dict()
        # this ain't right . this is backwards
        for name in self._lmt_map:
            if name = "window":
            for k,v in self._lmt_map[name].items():
                print(f"{name}{v}={k}")
                if not self.is_recognized_field(k):
                   raise KeyError('{k} is not a recognized dataset field in {self.name}')
                if k in self._metadata:
                    foo[v] = self._metadata[k]
                elif k in self._metadata["band"][0]:
                # ugh force to slBand 1
                    foo[v] = self._metadata["band"][0][k]
                else:
                # the key is valid but was not present
                    pass
                    
        return foo
        

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

def example():
    '''Example usage of LmtMetadataBlock'''
    lmtdata = LmtMetadataBlock()
    lmtdata.add_metadata("projectID","2021-S1-US-3")
    lmtdata.add_metadata("projectTitle","Life, the Universe, and Everything")
    lmtdata.add_metadata("PIName","Marc Pound")
    lmtdata.add_metadata("obsnum","12345") 
#   for multiple obsnums:
    lmtdata.add_metadata("obsnum","12345,56783,42099") 
    lmtdata.add_metadata("RA",14.01,"degree")
    lmtdata.add_metadata("DEC",-43.210)
    # add a band
    band = dict()
    band["slBand"] = 1
    band["lineName"]='CS2-1'
#   for multiple lines:
    #band["lineName"] = 'CS2-1,CO1-0,H2CS'
    band["frequencyCenter"] = 97981*u.Unit("MHz")
    band["bandwidth"] = 2.5
    band["beam"] = u.Quantity(20.0,"arcsec")
    band["lineSens"] = 0.072
    band["qaGrade"] = "A+++"
    lmtdata.add_metadata("band",band)
    # add a second band
    band["slBand"] = 2
    band["lineName"]='CO1-0'
#   for multiple lines:
    #band["lineName"] = 'CS2-1,CO1-0,H2CS'
    band["frequencyCenter"] = u.Quantity(115.2712,"GHz")
    band["bandwidth"] = 2.5 #GHz
    band["beam"] = (97.981/115.2712)*20.0/3600.0
    band["lineSens"] = 123*u.Unit("mK")
    band["qaGrade"] = "B-"
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
    print(lmtdata.to_yaml())
    return lmtdata


class CitationMetadataBlock(MetadataBlock):
    def __init__(self):
      self._datacsv = utils.aux_file("CitationMetaDatablock.csv")
      self._vocabcsv =  utils.aux_file("CitationControlledVocabulary.csv")
      super().__init__("CitationData",self._datacsv,self._vocabcsv)
      self._version = "Dataverse 5.12.1"

if __name__ == "__main__":

    lmtdata = example()
    if False: 
        print(lmtdata._has_units("bandwidth"))
        print(lmtdata._has_units("PIName"))
        print(lmtdata.get_units("bandwidth"))
        print(lmtdata.get_units("PIName"))
        print(lmtdata.get_units("band"))
    
