import pandas as pd
import json
import dvpipe.utils as utils
from copy import deepcopy

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
        self._datasetFields.set_index("parent")
        if vocabulary_file is not None:
            self._controlledVocabulary = pd.read_csv(vocabulary_file)
        # The actual metadata
        self._metadata = dict()
        self._version = None
    
    @property
    def name(self):
        '''Name of this metadata block'''
        return self._name

    @property 
    def datasetColnames(self):
        return self._dsfColnames

    @property
    def datasetFields(self):
        return self._datasetFields

    @property 
    def controlledVocabularyColnames(self):
        return self._cvColnames

    @property
    def controlledVocabulary(self):
        return self._controlledVocabulary

    @property 
    def metadata(self):
        return self._metadata

    @property
    def version(self):
        return self._version
 
    def add_metadata(self,name,value):
        if name not in self._datasetFields['name'].values:
            raise KeyError(f'{name} is not a recognized dataset field in {self.name}')
        # check value against controlled vocabulary
        if not self._check_controlled(name,value):
            s =  self._allowed_values(name,value)
            raise ValueError(f'{value} is not a valid value for dataset field {name} in {self.name}. Allowed values are: {s}.')
        isparent = self._is_parent(name)
        if isparent  and type(value) is not dict:
            raise ValueError(f'Dataset field {self.name} has children whose values must be passed as a dict: {self.get_children(name)}')
        if self._has_parent(name):
            parent = self.get_parent(name)
            raise ValueError(f'Dataset field "{name}" is a child of "{parent}" and passed as a dict member for "{parent}". e.g. add_metadata({parent},{{"{name}":...}}')
        if isparent:
            # Note: must use deepcopy of dict because caller may reuse the
            # the dict in calling object and this is normally just a reference.
            if name in self._metadata:
                self._metadata[name].append(deepcopy(value))
            else:
                self._metadata[name]= [deepcopy(value)]
        else:
            self._metadata[name] = value

    def _allowed_values(self,name,value):
        '''The allowed values of a variable if in a controlled vocabulary 
           (i.e., the enums).  Will return empty list if the variable 
           is not controlled
        '''
        series =  self._controlledVocabulary.loc[self._controlledVocabulary["DatasetField"] == name]["Value"]
        return series.values

    def _check_controlled(self,name,value):
        s =  self._allowed_values(name,value)
        return s.size == 0 or value in s

    def _has_parent(self,name):
        df = self._datasetFields[self._datasetFields['name'] == name]
        return df.notna()["parent"].values[0]

    def _is_parent(self,name):
        df = self._datasetFields[self._datasetFields['name'] == name]
        return df["fieldType"].values[0] == "none"

    def get_parent(self,name):
        df = self._datasetFields[self._datasetFields['name'] == name]
        return df["parent"].values[0]

    def get_children(self,name):
        '''Identify the children of the given dataset field
        
        :param name: the parent field to check for children
        :type name: str
        :return: list of names of children. This list will be empty if the input field has no children
        :rtype: numpy.ndarray
        '''
        df = self._datasetFields[self._datasetFields['parent'] == name]
        return df['name'].values

    def to_yaml(self):
        comment = f"# {self.name} metadata block version {self.version}"
        return comment+utils.pformat_yaml(self._metadata)

    def to_json(self,indent=4):
        comment = f"# {self.name} metadata block version {self.version}\n"
        return comment+json.dumps(self._metadata,indent=indent)
