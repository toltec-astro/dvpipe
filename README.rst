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
If you need to add a metadata item to the LMT 

Features
--------

* TODO

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
