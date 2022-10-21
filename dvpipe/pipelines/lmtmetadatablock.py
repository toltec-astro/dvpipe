from dvpipe.pipelines.metadatablock import MetadataBlock
import pandas as d
import json
import dvpipe.utils as utils

class LmtMetadataBlock(MetadataBlock):
    def __init__(self):
      self._datacsv = utils.aux_file("LMTMetaDatablock.csv")
      self._vocabcsv =  utils.aux_file("LMTControlledVocabulary.csv")
      super().__init__("LMTData",self._datacsv,self._vocabcsv)
      self._version = "1.0.2"

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
    lmtdata.add_metadata("RA",123.456)
    lmtdata.add_metadata("DEC",-43.210)
    # add a band
    band = dict()
    band["slBand"] = 1
    band["lineName"]='CS2-1'
#   for multiple lines:
    #band["lineName"] = 'CS2-1,CO1-0,H2CS'
    band["frequencyCenter"] = 97.981
    band["bandwidth"] = 2.5
    band["beam"] = 20.0/3600.0
    band["lineSens"] = 0.072
    band["qaGrade"] = "A+++"
    lmtdata.add_metadata("band",band)
    # add a second band
    band["slBand"] = 2
    band["lineName"]='CO1-0'
#   for multiple lines:
    #band["lineName"] = 'CS2-1,CO1-0,H2CS'
    band["frequencyCenter"] = 115.2712
    band["bandwidth"] = 2.5
    band["beam"] = (97.981/115.2712)*20.0/3600.0
    band["lineSens"] = 0.1
    band["qaGrade"] = "B-"
    lmtdata.add_metadata("band",band)

    lmtdata.add_metadata("obsDate",utils.now())
    lmtdata.add_metadata("intTime",30.0)
    lmtdata.add_metadata("velocity",321.0)
    lmtdata.add_metadata("velDef","RADIO")
    lmtdata.add_metadata("velFrame","LSR")
    lmtdata.add_metadata("velType","FREQUENCY")
    lmtdata.add_metadata("z",0.001071)
    lmtdata.add_metadata("observatory","LMT")
    lmtdata.add_metadata("LMTInstrument","SEQUOIA")
    lmtdata.add_metadata("targetName","NGC 5948")
    # YAML output
    print(lmtdata.to_yaml())

if __name__ == "__main__":

    example()
