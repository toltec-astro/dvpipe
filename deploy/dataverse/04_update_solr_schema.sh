#!/bin/bash

if [[ ! $1 ]]; then
    echo "Usage: $0 <dataverse_src_repo_path>"
    exit 1
fi
dvsrc=$1
solr_ver=9.3.0

curl "http://localhost:8080/api/admin/index/solr/schema" | \
        ${dvsrc}/conf/solr/${solr_ver}/update-fields.sh \
        /usr/local/solr/server/solr/collection1/conf/schema.xml
curl "http://localhost:8983/solr/admin/cores?action=RELOAD&core=collection1"
