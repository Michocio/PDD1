#!/bin/bash

wget -N . ftp://ftp.task.gda.pl/pub/www/apache/dist/hadoop/common/hadoop-2.8.3/hadoop-2.8.3.tar.gz

tar -xvf hadoop-2.8.3.tar.gz

cd hadoop-2.8.3
HADOOP_PREFIX=$(pwd)
export HADOOP_PREFIX
sed -i -e "s|^export JAVA_HOME=\${JAVA_HOME}|export JAVA_HOME=$JAVA_HOME|g" ${HADOOP_PREFIX}/etc/hadoop/hadoop-env.sh

cd ${HADOOP_PREFIX}
mkdir namenode
mkdir datanode

cat <<EOF > ${HADOOP_PREFIX}/etc/hadoop/core-site.xml
<configuration>
<property>
<name>fs.defaultFS</name>
<value>hdfs://localhost:9000</value>
</property>
</configuration>
EOF

cat <<EOF > ${HADOOP_PREFIX}/etc/hadoop/hdfs-site.xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>${HADOOP_PREFIX}/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>${HADOOP_PREFIX}/datanode</value>
  </property>
</configuration>
EOF

cat <<EOF > ${HADOOP_PREFIX}/etc/hadoop/yarn-site.xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>

    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>localhost</value>
    </property>
	<property>
	    <name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name>
	    <value>98.5</value>
	</property>
	<property>
	    <name>yarn.nodemanager.disk-health-checker.enable</name>
	    <value>false</value>
	</property>
</configuration>
EOF



cat <<EOF > ${HADOOP_PREFIX}/etc/hadoop/mapred-site.xml
<configuration>
<property>
<name>mapreduce.framework.name</name>
<value>yarn</value>
</property>
</configuration>
EOF


cd ${HADOOP_PREFIX}
bin/hdfs namenode -format

sbin/start-dfs.sh
sbin/start-yarn.sh

bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/mj348711


bin/hdfs dfs -put input /user/mj348711












