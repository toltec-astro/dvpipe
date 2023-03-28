# Dataverse deployment notes


## Add LMT metadata bloack

* Create the "resource_bundle" file from the metadata definition TSV:

    00_make_rb.py lmtdata_metadatablock.tsv

* Upload the metadatablock TSV file to dataverse:

    02_load_datasetfield.sh

* Register new metadata to the solr service:

    04_update_solr_schema.sh

* Copy the resource bundle file ".resource_bundle" to glassfish:

    cp lmtdata_metadatablock.resource_bundle /usr/local/payara5/glassfish/domains/domain1/applications/dataverse/WEB-INF/classes/propertyFiles/LMTData.properties 
