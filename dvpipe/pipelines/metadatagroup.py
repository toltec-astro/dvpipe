from lmtmetadatablock import LmtMetadataBlock, CitationMetadataBlock
import dvpipe.utils as utils
import astropy.units as u
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
                print(f"added {name}={value} to {b.name}")
                return
        raise KeyError(f'{name} is not a recognized dataset field in metadatablocks: {list(self._blocks.keys())}')
        
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

    @property
    def keys(self):
        s = ''
        for b in self._blocks.values():
            s+=f'{b.name}: {b.keys}\n'
        return s

def example():
    lmt = LmtMetadataGroup("LMT Group")
    print(lmt.keys)

    # Citation metadata
    desc = dict()

    desc["dsDescriptionValue"] = "Combined reduction of obsnums 12345, 56783, 42099."
    desc["dsDescriptionDate"] = utils.now()
    lmt.add_metadata("dsDescription",desc)
    lmt.add_metadata("subject","Astronomy and Astrophysics")
    contact = dict()
    contact["datasetContactName"] = "Ma, Zhiyuan"
    contact["datasetContactAffiliation"] = "University of Massachusetts"
    contact["datasetContactEmail"] = "zhiyuanma@umass.edu"
    lmt.add_metadata("datasetContact",contact)
    lmt.add_metadata("depositor","LMT Pipeline")
    lmt.add_metadata("dateOfDeposit",utils.now())
    # "softwareName", "softwareVersion"

    #LMT Metadata 
    lmt.add_metadata("projectID","2021-S1-US-3")
    lmt.add_metadata("projectTitle","Life, the Universe, and Everything")
    lmt.add_metadata("PIName","Marc Pound")
    lmt.add_metadata("obsnum","12345") 
#   fortiple obsnums:
    lmt.add_metadata("obsnum","12345,56783,42099") 
    lmt.add_metadata("RA",14.01,"degree")
    lmt.add_metadata("DEC",-43.210)
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
    lmt.add_metadata("band",band)
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
    lmt.add_metadata("band",band)

    lmt.add_metadata("obsDate",utils.now())
    lmt.add_metadata("intTime",30.0,"minute")
    lmt.add_metadata("velocity",321.0,u.Unit("m/s"))
    lmt.add_metadata("velDef","RADIO")
    lmt.add_metadata("velFrame","LSR")
    lmt.add_metadata("velType","FREQUENCY")
    lmt.add_metadata("z",0.001071)
    lmt.add_metadata("observatory","LMT")
    lmt.add_metadata("LMTInstrument","SEQUOIA")
    lmt.add_metadata("targetName","NGC 5948")
    # YAML output
    print(lmt.to_yaml())
    return lmt
