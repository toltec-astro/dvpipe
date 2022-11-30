from lmtmetadatablock import LmtMetadataBlock, CitationMetadataBlock
class MetadataGroup(object):
    '''Object to contain multiple metadata blocks and write them as one yaml file'''
    def __init__(self,name):
        self._name = name
        self._blocks = dict()

    @property 
    def name(self):
        return self._name

    @property 
    def blocks(self):
        return self._blocks

    def add_block(self,key,block):
        self._blocks[key] = block

    def add_metadata(self,name,value,units=None):
        for b in self._blocks.values():
            if b.is_recognized_field(name):
                b.add_metadata(name,value,units)
        raise KeyError(f'{name} is not a recognized dataset field in metadatablocks: list(self._blocks.keys())')
        
    def to_yaml(self):
        retval = ''
        for b in self._blocks.values():
            retval += b.to_yaml()
        return retval

class LmtMetadataGroup(MetadataGroup):
    def __init__(self,name):
        super().__init__(name)
        self.add_block("LMT",LmtMetadataBlock())
        self.add_block("citation",CitationMetadataBlock())
