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

if __name__ == "__main__":
    lmtdata = LmtMetadataBlock()
    print(lmtdata.name,'\n',lmtdata.datasetFields)
    print("LMTMetadata version",lmtdata.version)
    print(lmtdata.datasetFields['name'].values)
    lmtdata.add_metadata("projectID","2021-S1-US-3")
    lmtdata.add_metadata("projectTitle","Life, the Universe, and Everything")
    lmtdata.add_metadata("PIName","Marc Pound")
    lmtdata.add_metadata("obsnum","12345") 
#   for multiple obsnums:
    lmtdata.add_metadata("obsnum","12345,56783,42099") 
    lmtdata.add_metadata("RA",123.456)
    lmtdata.add_metadata("DEC",-43.210)
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
    try:
        lmtdata.add_metadata("foobar",12345)
    except KeyError as v:
        print("Caught as expected: ",v)
    print(lmtdata.controlledVocabulary)
    print(lmtdata._check_controlled("velFrame","Foobar"))
    print(lmtdata._check_controlled("velFrame","LSR"))
    print(lmtdata._check_controlled("foobar","uhno"))
    try:
        lmtdata.add_metadata("velFrame","Foobar")
    except ValueError as v:
        print("Caught as expected: ",v)
    print(lmtdata._has_parent("slBand"))
    print(lmtdata._has_parent("velFrame"))
    print(lmtdata._is_parent("band"))
    print(lmtdata.get_children("band"))
    print(lmtdata.get_children("targetName"))

    print("YAML")
    print(lmtdata.to_yaml())
    print("JSON")
    print(lmtdata.to_json())

    
