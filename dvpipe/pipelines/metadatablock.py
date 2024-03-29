import pandas as pd
import json
import yaml
#import pyaml
#import ruamel.yaml as ry
import dvpipe.utils as utils
from copy import deepcopy
import astropy.units as u
from numbers import Number
#import numpy as np

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
                          'required', 'parent', 'metadatablock_id','units']
        self._cvColnames = ['DatasetField', 'Value',
                            'identifier', 'displayOrder']
        # metdata definition
        self._dataset_file = dataset_file
        # controlled vocabulary definition
        self._vocabulary_file = vocabulary_file
        #TODO trim trailing spaces will will get us intro trouble possibly later
        self._datasetFields = pd.read_csv(dataset_file,skipinitialspace=True)
        self._datasetFields.set_index("parent")
        if vocabulary_file is not None:
            self._controlledVocabulary = pd.read_csv(vocabulary_file,skipinitialspace=True)
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
    def keys(self):
        return list(self._datasetFields['name'])

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

    def is_recognized_field(self,name):
        return name in self._datasetFields['name'].values

    def add_metadata(self,name,value,units=None):
        if not self.is_recognized_field(name):
            raise KeyError(f'{name} is not a recognized dataset field in {self.name}')
        # check parent-child relationship of inputs
        isparent = self._is_parent(name)
        if isparent  and type(value) is not dict:
            raise ValueError(f'Dataset field {self.name} has children whose values must be passed as a dict: {self.get_children(name)}')
        if self._has_parent(name):
            parent = self.get_parent(name)
            raise ValueError(f'Dataset field "{name}" is a child of "{parent}" and passed as a dict member for "{parent}". e.g. add_metadata({parent},{{"{name}":...}}')

        value_checked = self._check_units(name,value,units)

        for k,v in value_checked.items():
            if not self._check_controlled(k,v):
                s =  self._allowed_values(k,v)
                raise ValueError(f'{v} is not a valid value for dataset field {k} in {self.name}. Allowed values are: {s}.')

        if isparent:
            # Note: must use deepcopy of dict because caller may reuse the
            # the dict in calling object and this is normally just a reference.
            if name in self._metadata:
                self._metadata[name].append(deepcopy(value_checked))
            else:
                self._metadata[name]= [deepcopy(value_checked)]
        else:
         # prevent yaml writer from putting quotes around it because
         # it is <'class numpy.float64'>
            if type(value_checked[name]) is str:
                self._metadata[name] = value_checked[name]
            else:
                self._metadata[name] = float(value_checked[name])

    def is_controlled(self,name):
        av = self._allowed_values(name)
        return len(av) != 0
        
    def _allowed_values(self,name,value=None): #value is not needed here - fix calls!
        '''The allowed values of a variable if in a controlled vocabulary
           (i.e., the enums).  Will return empty list if the variable
           is not controlled
        '''
        series =  self._controlledVocabulary.loc[self._controlledVocabulary["DatasetField"] == name]["Value"]
        return series.values

    def _check_controlled(self,name,value):
        '''Check that `value` is allowable for key `name`.  If `name` is governed by a controlled vocabulary, `value` must be in the vocabulary.

           :param name:  The name of the dataset field
           :type name: str

           :param value: The value of the dataset field
           :type name: any

           :rtype: bool
           :return: True if `value` is in a controlled vocabulary of `name` or is not controlled. False otherwise.
        '''
        s =  self._allowed_values(name,value)
        return s.size == 0 or value in s

    def _check_units(self,name,value,units):
        '''always returns a dict with key=name, value is value in defined units'''
        #print(f"checking units for {name},{value},{units}")
        requnits = self.get_units(name)
        parsed_dict = dict()
        if type(value) is dict:
            if units is not None:
                raise ValueError("You can't provide units if value is a dict; the dict must contain astropy Quantities")
            for k,v in value.items():
                #print("checking ",k,v)
                q = self._check_units(k,v,units=None)
                parsed_dict[k] = q[k]
            return parsed_dict

        if requnits is not None:
          try :
            if units is None:
                q = u.Quantity(value,requnits)
            else:
                q = u.Quantity(value,units).to(requnits)
          except Exception as ex:
            raise ValueError(f'Error converting units for {name}: {ex}. Required units are {requnits}.')
          parsed_dict[name] = q.value
          if type(parsed_dict[name]) is not str:
            parsed_dict[name] =  parsed_dict[name].item()
        else:
          parsed_dict[name] = value
        return parsed_dict

    def _has_units(self,name):
        # duh use is recognized data
        df = self._datasetFields[self._datasetFields['name'] == name]
        if df.empty:
            raise Exception(f'{name} is not a valid dataset field')
        return not df["units"].dropna().empty

    def get_units(self,name):
        df = self._datasetFields[self._datasetFields['name'] == name]
        if df.empty:
            raise Exception(f'{name} is not a valid dataset field')
        if "units" not in df.columns or pd.isnull(df["units"].iloc[0]):
            return None
        else:
            return df["units"].iloc[0]

    def _has_parent(self,name):
        df = self._datasetFields[self._datasetFields['name'] == name]
        if df.empty:
            raise Exception(f'{name} is not a valid dataset field')
        return df.notna()["parent"].values[0]

    def _is_parent(self,name):
        df = self._datasetFields[self._datasetFields['name'] == name]
        if df.empty:
            raise Exception(f'{name} is not a valid dataset field')
        return df["fieldType"].values[0] == "none"

    def get_parent(self,name):
        df = self._datasetFields[self._datasetFields['name'] == name]
        if df.empty:
            raise Exception(f'{name} is not a valid dataset field')
        return df["parent"].values[0]

    def get_children(self,name):
        '''Identify the children of the given dataset field

        :param name: the parent field to check for children
        :type name: str
        :return: list of names of children. This list will be empty if the input field has no children
        :rtype: numpy.ndarray
        '''
        df = self._datasetFields[self._datasetFields['parent'] == name]
        if df.empty:
            raise Exception(f'{name} is not a valid dataset field')
        return df['name'].values

    def to_yaml(self):
        comment = f"# {self.name} metadata block version {self.version}"
        return comment+utils.pformat_yaml(self._metadata)
    #def to_yaml2(self):
    #    comment = f"# YAML2 {self.name} metadata block version {self.version}"
    #    return yaml.dump(self._metadata,encoding='utf-8')
    #def to_yaml3(self):
    #    return pyaml.p(self._metadata)

    def to_json(self,indent=4):
        comment = f"# {self.name} metadata block version {self.version}\n"
        return comment+json.dumps(self._metadata,indent=indent)

    def __str__(self):
        return f"MetadataBlock({self.name})"

    def to_dataverse_dict(self):
        return "Subclasses must implement this!"

    def to_dataverse_dataset_fields(self):
        # this function should prepare the data in this way:
        # https://guides.dataverse.org/en/5.10.1/api/native-api.html#create-dataset-command
        # see "data.LMTData" here for the expected data:
        # http://lmtdv1.astro.umass.edu/api/datasets/2/versions/1/metadata
        d = self._metadata
        return self.to_dataverse_dict()

    def load_from_yaml(self, yamlfile):
        with open(yamlfile, 'r') as fo:
            d = utils.yaml.load(fo)
        self._metadata = d

    def validate(self):
        '''Return true if the metadata are present and valid'''
        return len(self.missing_metadata()) == 0

    def missing_metadata(self):
        '''Check if any key,value pairs are missing from metadata or if any values are None.  
      
        :returns: list with key names of missing metadata. Empty if none missing
        '''
        missing = []
        for k in self._datasetFields['name'].values:
            value = self._metadata.get(k,None)
            #print(k,type(value))
            if value is None and not self._has_parent(k):
                missing.append(k)
            if self._is_parent(k):
                if type(value) is list:
                # it will be a list of dictionaries
                    for v in value:
                        children = self.get_children(k)
                        for c in children:
                            if v.get(c,None) is None:
                                missing.append(c)
                else:
                    print(f"Warning: got unexpected value for {k}={value}")
                    missing.append(k)
        return missing
