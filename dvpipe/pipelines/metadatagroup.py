from dvpipe.pipelines.lmtmetadatablock import LmtMetadataBlock, CitationMetadataBlock
import dvpipe.utils as utils
import astropy.units as u
from astropy.coordinates import SkyCoord

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
                return
        raise KeyError(f'{name} is not a recognized dataset field in metadatablocks: {list(self._blocks.keys())}')
        
    def to_yaml(self):
        retval = ''
        for b in self._blocks.values():
            retval += b.to_yaml()
        return retval

    def to_json(self):
        retval = ''
        for b in self._blocks.values():
            retval += b.to_json()
        return retval

    def dbfile(self,dbfile):
        self._blocks["LMT"]._dbfile = dbfile

    def yamlfile(self,yamlfile):
        self._blocks["LMT"]._yamlfile = yamlfile

    def write_to_db(self):
        self._blocks["LMT"]._write_to_db()

    def write_to_yaml(self):
        self._blocks["LMT"]._write_to_yaml()


class LmtMetadataGroup(MetadataGroup):
    def __init__(self,name,dbfile=None,yamlfile=None):
        super().__init__(name)
        self.add_block("LMT",LmtMetadataBlock(dbfile=dbfile,yamlfile=yamlfile))
        self.add_block("citation",CitationMetadataBlock())

    @property
    def keys(self):
        s = ''
        for b in self._blocks.values():
            s+=f'{b.name}: {b.keys}\n'
        return s

def example(dbfile=None,yamlfile=None):
    lmt = LmtMetadataGroup("LMT Group",dbfile=dbfile,yamlfile=yamlfile)
    #print(lmt.keys)

    # Dataverse Citation metadata
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

    # LMT specific metadata
    lmt.add_metadata("projectID","2021-S1-US-3")
    lmt.add_metadata("projectTitle","Life, the Universe, and Everything")
    lmt.add_metadata("PIName","Marc Pound")
    # example with obsnum range and full obsnum list.
    lmt.add_metadata("obsnum","12345_12350") 
    lmt.add_metadata("obsnumList","12345,12346,12347,12348,12349,12350") 
    coord = SkyCoord(ra=14.01*u.degree,dec=-43.21*u.degree,frame='icrs')
    lmt.add_metadata("RA",coord.ra.value,coord.ra.unit)
    lmt.add_metadata("DEC",coord.dec.value,coord.dec.unit)
    lmt.add_metadata("galLon",coord.galactic.l.value,coord.galactic.l.unit)
    lmt.add_metadata("galLat",coord.galactic.b.value,coord.galactic.b.unit)
    # add a band
    band = dict()
    band["slBand"] = 1
    band["formula"]='CS'
    band["transition"]='2-1'
    band["frequencyCenter"] = 97981*u.Unit("MHz")
    band["velocityCenter"] = 300.0 #km/s
    band["bandwidth"] = 2.5
    band["beam"] = u.Quantity(20.0,"arcsec")
    band["lineSens"] = 0.072
    band["contSens"] = 0.001
    band["qaGrade"] = "A+++"
    band["nchan"] = 1024
    lmt.add_metadata("band",band)
    # add a second band
    band["slBand"] = 2
    band["formula"]='CO'
    band["transition"]='1-0'
#   for multiple lines:
    band["frequencyCenter"] = u.Quantity(115.2712,"GHz")
    band["bandwidth"] = 2.5 #GHz
    band["beam"] = (97.981/115.2712)*20.0/3600.0
    band["lineSens"] = 123*u.Unit("mK")
    band["qaGrade"] = "B-"
    band["velocityCenter"] = -25.0 #km/s
    band["nchan"] = 2048
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
    # 0 = uncalibrated, 1 = calibration level 1, etc
    lmt.add_metadata("calibrationLevel",1)
    lmt.add_metadata("obsGoal","SCIENCE")
    lmt.add_metadata("obsComment","This is an observation comment")
    lmt.add_metadata("boundingBox","[]")
    lmt.add_metadata("pipeVersion","1.0")

    return lmt

if __name__ == "__main__":
    lmtdata = example(dbfile="example_lmt.db",yamlfile="example_lmt.yaml") 
    lmtdata.write_to_db()
    lmtdata.write_to_yaml()

    # Can't write if dbfile is none
    lmtdata.dbfile(None)
    lmtdata.write_to_db()
    # Can't write if yaml is none
    lmtdata.yamlfile(None)
    lmtdata.write_to_yaml()
    lmtdata.write_to_json()
