set -e
# MINIO_RPM_PATH=https://dl.min.io/server/minio/release/linux-amd64/minio-20230816201730.0.0.x86_64.rpm
MINIO_RPM_PATH=https://dl.min.io/server/minio/release/linux-amd64/minio.rpm

MINIO_USER=minio-user
MINIO_ROOT_PATH=/data_minio

# install
dnf install ${MINIO_RPM_PATH}

# setup
groupadd -r ${MINIO_USER}
useradd -M -r -g ${MINIO_USER} ${MINIO_USER}

if [ ! -e ${MINIO_ROOT_PATH} ]; then
	mkdir -p ${MINIO_ROOT_PATH}
fi
chown ${MINIO_USER}:${MINIO_USER} ${MINIO_ROOT_PATH}

