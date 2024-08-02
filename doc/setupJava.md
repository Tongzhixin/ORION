### sdk install
curl -s https://get.sdkman.io | bash
source "/root/.sdkman/bin/sdkman-init.sh"
sdk install java 22.0.1-tem


### hadoop

1. install hadoop
   ```
   wget https://dlcdn.apache.org/hadoop/common/hadoop-2.10.2/hadoop-2.10.2.tar.gz -O /opt/download/hadoop-2.10.2.tar.gz
   wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz -O /opt/download/hadoop-3.3.4.tar.gz
   tar -zxvf /opt/download/hadoop-3.3.4.tar.gz -C /opt/software/

   # vim etc/hadoop/hadoop-env.sh
   export JAVA_HOME=/root/.sdkman/candidates/java/current
   ```

2. configure `etc/hadoop/core-site.xml`
   ```
   <configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://master:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/root/tmp</value>
    </property>
    <property>
   <name>io.file.buffer.size</name>
   <value>40960</value>
    </property>
       <property>
     <name>fs.s3a.access.key</name>
     <value></value>
     <description>AWS access key ID.
      Omit for IAM role-based or provider-based authentication.</description>
      </property>

   <property>
     <name>fs.s3a.secret.key</name>
     <value></value>
      Omit for IAM role-based or provider-based authentication.</description>
   </property>
       <property>
           <name>fs.s3a.endpoint</name>
           <value>https://xxx</value>
       </property>
               <property>
           <name>fs.s3a.endpoint.region</name>
           <value></value>
       </property>

   <property>
     <name>fs.s3a.aws.credentials.provider</name>
   <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
   </property> 
   </configuration>
   ```
3. configure `etc/hadoop/hdfs-site.xml` as offical
4. modify the bashrc

   ```
   export HADOOP_CONF_DIR=/opt/software/hadoop-2.10.2/etc/hadoop
   export HADOOP_HOME=/opt/software/hadoop-2.10.2
   export HIVE_HOME=/opt/software/hive
   export HDFS_NAMENODE_USER=root
   export HDFS_DATANODE_USER=root
   export HDFS_SECONDARYNAMENODE_USER=root
   export YARN_RESOURCEMANAGER_USER=root
   export YARN_NODEMANAGER_USER=root
   ```

5. The command of hadoop
   1. start
   ```
   $HADOOP_HOME/bin/hdfs namenode -format
   $HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
   $HADOOP_HOME/bin/hdfs dfsadmin -report
   $HADOOP_HOME/sbin/start-dfs.sh
   ```
   2. stop
    `$HADOOP_HOME/sbin/stop-dfs.sh`

### hive

1. Installation
    ```
    wget https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz -O /opt/download/apache-hive-3.1.3-bin.tar.gz
   tar -zxvf /opt/download/apache-hive-3.1.3-bin.tar.gz -C /opt/software/
   export HIVE_HOME=`pwd`
   cp conf/hive-default.xml.template conf/hive-site.xml
    ```
    edit the `conf/hive-env.sh`
    ```
    export HIVE_AUX_JARS_PATH=/opt/software/hadoop-2.10.2/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.271.jar:/opt/software/hadoop-2.10.2/share/hadoop/tools/lib/hadoop-aws-2.10.2.jar
    export AWS_ACCESS_KEY_ID=
    export AWS_SECRET_ACCESS_KEY=
    ```
2. The command
   start
    ```
    rm -rf hcatalog/var/log/
   mkdir -p hcatalog/var/log/
   rm -rf metastore_db
   $HIVE_HOME/bin/schematool -dbType derby -initSchema
    ```
    stop 
    ```
    $HIVE_HOME/hcatalog/sbin/hcat_server.sh stop
    ```