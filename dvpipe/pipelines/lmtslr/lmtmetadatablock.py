from metadatablock import MetadataBlock
import pandas as pd
import json
import dvpipe.utils as utils

class LmtMetadataBlock(MetadataBlock):
    def __init__(self):
      self._datacsv = utils.aux_file("LMTMetaDatablock.csv")
      self._vocabcsv =  utils.aux_file("LMTControlledVocabulary.csv")
      super().__init__("LMTData",self._datacsv,self._vocabcsv)

if __name__ == "__main__":
    lmtdata = LmtMetadataBlock()
    print(lmtdata.name,'\n',lmtdata.datasetFields)
    print(type(lmtdata._datasetFields))
    print(lmtdata.datasetFields['name'].values)
    lmtdata.add_metadata("projectID","2021-S1-US-3")
    lmtdata.add_metadata("PIName","Marc Pound")
    lmtdata.add_metadata("obsnum",12345)
    lmtdata.add_metadata("RA",123.456)
    lmtdata.add_metadata("DEC",-43.210)
    lmtdata.add_metadata("slBand",1)
    lmtdata.add_metadata("lineName",'CS2-1')
    lmtdata.add_metadata("frequencyCenter",97.981)
    lmtdata.add_metadata("bandwidth",2.5)
    lmtdata.add_metadata("intTime",30.0)
    lmtdata.add_metadata("projectTitle","Life, the Universe, and Everything")
    lmtdata.add_metadata("obsDate",utils.now())
    lmtdata.add_metadata("velocity",321.0)
    lmtdata.add_metadata("velDef","RADIO")
    lmtdata.add_metadata("velFrame","LSR")
    lmtdata.add_metadata("velType","FREQUENCY")
    lmtdata.add_metadata("z",0.001071)
    lmtdata.add_metadata("beam",20.0/3600.0)
    lmtdata.add_metadata("lineSens",0.072)
    lmtdata.add_metadata("facility","LMT")
    lmtdata.add_metadata("instrument","SEQUOIA")
    lmtdata.add_metadata("object","NGC 5948")
    try:
        lmtdata.add_metadata("foobar",12345)
    except KeyError as v:
        print("Caught as expected: ",v)
#    print(lmtdata.metadata)
    js = json.dumps(lmtdata.metadata,indent=4)
    print(js)
    print(utils.pformat_yaml(js))
    print(lmtdata.controlledVocabulary)
    print(lmtdata.check_controlled("velFrame","Foobar"))
    print(lmtdata.check_controlled("velFrame","LSR"))
    print(lmtdata.check_controlled("foobar","uhno"))
    lmtdata.add_metadata("velFrame","Foobar")

