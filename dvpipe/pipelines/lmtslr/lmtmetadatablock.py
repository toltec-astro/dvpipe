from metadatablock import MetadataBlock
import pandas as pd
import dvpipe.utils as utils

class LmtMetadataBlock(MetadataBlock):
    def __init__(self):
      self._datacsv = utils.auxfile("LMTMetaDatablock.csv")
      self._vocabcsv =  utils.auxfile("LMTControlledVocabulary.csv")
      super().__init("LMTData",self._datacsv,self._vocabcsv)
