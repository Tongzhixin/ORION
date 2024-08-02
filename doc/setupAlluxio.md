### alluxio
docker network create alluxio_network
```
docker run -dit --cap-add=SYS_ADMIN --cap-add=NET_ADMIN --cap-add=NET_RAW\
    -p 18999:19999 \
    -p 19998:19998 \
    --net=alluxio_network \
    --name=alluxio-master \
    -v /state/partition/zxtong/tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=alluxio-master \
       -Dalluxio.user.ufs.block.read.concurrency.max=100 \
       -Dalluxio.underfs.s3.disable.dns.buckets=true \
       -Dalluxio.security.authorization.permission.enabled=false \
       -Ds3a.accessKeyId=xxx \
       -Ds3a.secretKey=xxxxx \
       -Dalluxio.underfs.s3.endpoint=xxxx \
       -Dalluxio.master.mount.table.root.ufs=s3a://tpch-all/" \
    alluxio/alluxio master

docker run -dit --cap-add=SYS_ADMIN --cap-add=NET_ADMIN --cap-add=NET_RAW\
    -p 29999:29999 \
    -p 30000:30000 \
    --net=alluxio_network \
    --name=alluxio-worker \
    --shm-size=22G \
    -v /state/partition/zxtong/tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.mount.table.root.ufs=s3a://tpch-all/ \
       -Dalluxio.worker.memory.size=16G \
       -Dalluxio.security.authorization.permission.enabled=false \
       -Dalluxio.user.ufs.block.read.concurrency.max=100 \
       -Dalluxio.underfs.s3.disable.dns.buckets=true \
       -Ds3a.accessKeyId=xxx \
       -Ds3a.secretKey=xxx \
       -Dalluxio.underfs.s3.endpoint=http://xxxx \
       -Dalluxio.master.hostname=alluxio-master \
       -Dalluxio.worker.hostname=xxx" \
    alluxio/alluxio worker
```
The command follows as :
<!-- ./bin/alluxio clearCache -->
./bin/alluxio fs free /
docker stop alluxio-master && docker rm alluxio-master
docker stop alluxio-worker && docker rm alluxio-worker