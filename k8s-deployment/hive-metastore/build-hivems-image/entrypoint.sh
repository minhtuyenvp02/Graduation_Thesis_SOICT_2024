#!/bin/sh

# Setting up env variables needed for metastore final configuration
export HADOOP_HOME=/opt/hadoop-3.2.0
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar:${HADOOP_HOME}/share/hadoop/tools/lib/delta-hive-assembly_2.11-0.2.0.jar
export JAVA_HOME=/usr/local/openjdk-8

# Start the hive metastore server
/opt/apache-hive-metastore-3.0.0-bin/bin/schematool -upgradeSchema -dbType postgres
/opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore