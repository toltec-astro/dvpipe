#!/bin/sh
if [[ ! $1 ]] ; then
    echo "Usage: $0 <tsvfile>"
    exit 1
fi
curl http://localhost:8080/api/admin/datasetfield/load -H "Content-type: text/tab-separated-values" -X POST --upload-file $1
