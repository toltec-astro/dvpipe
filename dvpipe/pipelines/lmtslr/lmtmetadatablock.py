from metadatablock import MetadataBlock
import pandas as pd
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
    lmtdata.add_metadata("PIName","Marc Pound")
    lmtdata.add_metadata("slBand",1)
    try:
        lmtdata.add_metadata("foobar",12345)
    except KeyError as v:
        print("Caught as expected: ",v)
    print(lmtdata.metadata)
