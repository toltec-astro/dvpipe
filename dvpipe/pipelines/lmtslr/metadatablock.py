
import pandas as pd
class MetadataBlock(object):
    '''Generic representation of a Dataverse metadata block.
       Metadata blocks have a datasetField which gives the names and
       properties of the metadata items, i.e. the metadata definition. 
       Actual metadata conforming to this definition are in the 
      'metadata' proprty.
       Specific metadata blocks should inherit from this class.
    '''
    def __init__(self,name,dataset_file,vocabulary_file=None):
        self._name = name 
        self._dsfColnames = ['name', 'title', 'description', 'watermark',
                          'fieldType', 'displayOrder', 'displayFormat', 
                          'advancedSearchField', 'allowControlledVocabulary', 
                          'allowmultiples', 'facetable', 'displayoncreate', 
                          'required', 'parent', 'metadatablock_id']
        self._cvColnames = ['DatasetField', 'Value', 
                            'identifier', 'displayOrder']
        # metdata definition
        self._dataset_file = dataset_file
        # controlled vocabulary definition
        self._vocabulary_file = vocabulary_file
        self._datasetFields = pd.read_csv(dataset_file)
        if vocabulary_file is not None:
            self._controlledVocabulary = pd.read_csv(vocabulary_file)
        # The actual metadata
        self._metadata = dict()
    
    @property
    def name(self):
        return self._name

    @property 
    def datasetColnames(self):
        return self._dsfColnames

    @property
    def datasetFields(self):
        return self._datasetFields

    @property 
    def controlledVocularyColnames(self):
        return self._cvColnames

    @property
    def controlledVocabulary(self):
        return self._controlledVocabulary

    @property 
    def metadata(self):
        return self._metadata
 
    def add_metadata(self,name,value):
        if name not in self._datasetFields['name'].values:
            raise KeyError(f'{name} is not a recognized dataset field in {self.name}')
        #@todo check value against controlled vocabulary
        self._metadata[name] = value
