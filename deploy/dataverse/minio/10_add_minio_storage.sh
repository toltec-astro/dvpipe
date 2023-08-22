DV_MINIO_STORAGE_NAME=nese-minio
DV_MINIO_ENDPOINT_REGION=us-east-1
DV_MINIO_ENDPOINT_PROXY_URL=localhost:9000
DV_MINIO_ENDPOINT_URL=dp.lmtgtm.org
DV_MINIO_ACCESS_KEY=
DV_MINIO_SECRET_KEY=


./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.type=s3"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.label=${DV_MINIO_STORAGE}"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.bucket-name=${DV_MINIO_STORAGE}"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.upload-redirect=true"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.profile=default"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.download-redirect=true"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.ingestsizelimit=1000000000"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.url-expiration-minutes=120"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.path-style-access=true"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.custom-endpoint-region=${DV_MINIO_ENDPOINT_REGION}"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.proxy-url=${DV_MINIO_ENDPOINT_PROXY_URL}"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.custom-endpoint-url=${DV_MINIO_ENDPOINT_URL}"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.access-key=${DV_MINIO_ACCESS_KEY}"
./asadmin $ASADMIN_OPTS create-jvm-options "-Ddataverse.files.nese_minio.secret-key=${DV_MINIO_SECRET_KEY}"
