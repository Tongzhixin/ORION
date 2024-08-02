### Minio

1. Use Docker to deploy the Minio
```
docker run -dit --cap-add=NET_ADMIN --cap-add=NET_RAW  \
   -p 9100:9000 \
   -p 9101:9001 \
   --user $(id -u):$(id -g) \
   --name minio1 \
   -e "MINIO_ROOT_USER=ROOTUSER" \
   -e "MINIO_ROOT_PASSWORD=CHANGEME123" \
   -v /state/partition/zxtong/minio/data:/data \
   quay.io/minio/minio server /data --console-address ":9001"
```