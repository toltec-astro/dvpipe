======
dvpipe
======


.. image:: https://img.shields.io/pypi/v/dvpipe.svg
        :target: https://pypi.python.org/pypi/dvpipe

.. image:: https://img.shields.io/travis/toltec-astro/dvpipe.svg
        :target: https://travis-ci.com/toltec-astro/dvpipe

.. image:: https://readthedocs.org/projects/dvpipe/badge/?version=latest
        :target: https://dvpipe.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/toltec-astro/dvpipe/shield.svg
     :target: https://pyup.io/repos/github/toltec-astro/dvpipe/
     :alt: Updates



Dataverse integration for data pipelines.


* Free software: BSD license
* Documentation: https://dvpipe.readthedocs.io.

LMT Metadata Use and Modification
---------------------------------
The LMT metadata are defined in what is called a MetadataBlock in Dataverse.
Following Dataverse protocol, the fundamental definition of the LMT Metadatablock is in a tab-separated value (TSV) file which is ingested into Dataverse. In this codebase the TSV file is
``dvpipe/aux/LMT_Dataverse_Metadata_Definition.tsv``

The main code to create and write LMT metadata is in the MetaDataGroup class
 ``dvpipe/pipelines/metadatagroup.py``

Why group instead of block? Because the metadata to describe LMT data in the Dataverse
is actually a composite of two metadatablocks the LMT metadataBLock and
citation metadatablock.  The citation metadatablock is pre-defined by dataverse 

There are additional classes like LmtMetadataBlock that you should not have to use directly to manipulate LMT metadata.
In metadatagroup.py the method ``example()`` shows how you would go
about creating some LMT metadata and writing it to the YAML file that
gets uploaded to Dataverse with dvpipe.  The example also shows how to the sqlite
database that is used as the "frontend search" web-form for the LMT archive.

Adding to the LMT Metadatablock
===============================
If you need to add a new metadata item to the LMT Metadatablock, there are multiple files to update:

- *dvpipe/aux/LMT_Dataverse_Metadata_Definition.tsv*
    This Tab Separated Value file is the fundamental dataverse definition of the metadatablock.  This file gets uploaded to the dataverse to change the LMTMetadata definition. It consists of the metadata dataset fields table and the controlled vocabulary table.

- *dvpipe/aux/LMTMetaDatablock.csv* 
    This Comma Separated Value file is a copy of metadata dataset fields table from the TSV file.  The dvpipe metadata python code relies on this file to define the metadata.
 
- *dvpipe/aux/LMTControlledVocabulary.csv* 
    This CSV file is a copy of the controlled vocabulary table from the TSV files.The dvpipe metadata python code relies on this file to define the controlled vocabulary.

- *dvpipe/aux/alma_to_lmt_keymap.csv* 
    If the new metadata maps to an 'alma' keyword in astroquery/admit/core.py from the 'admit' branch of the astroquery git repo, the mapping should be listed in this CSV.

- *dvpipe/pipelines/metadb.py*
    If the new metadata maps to an 'alma' keyword the alma keyword must be put in the "alma_table" variable in metadb.py.

**Note:** Adding a new metadata item does **not** require updating lmtmetadatablock.py, metadatablockpy,  or metadatagroup.py.   They use the CSV files to define the metadata block. Generally, don't touch them unless new *functionality* is needed.

Features
------------

* TODO

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
