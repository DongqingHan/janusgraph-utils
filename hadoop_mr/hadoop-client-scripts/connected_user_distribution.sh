#!/bin/bash

MAIN_CLASS="ConnectedUserDistribution"
if [ -z "$HADOOP_HOME" ]; then
    export HADOOP=`which hadoop`
else
    export HADOOP="$HADOOP_HOME/bin/hadoop"
fi

basedir=`dirname "${BASH_SOURCE-$0}"`
jardir="/var/lib/hadoop-hdfs/handongqing/libjars"
libdir="/var/lib/hadoop-hdfs/handongqing/lib"
JAR=$(ls $jardir/anti-fraud*.jar)
dependencies=$(ls $libdir/*.jar | tr '\n' :)
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$dependencies
export LIBJARS=`echo ${dependencies} | sed s/:/,/g`
export JAVA_HOME=/usr/java/jdk


#hdfs dfs -rm -r -f /user/handongqing/user_distribution
$HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} \
    /user/handongqing/prod_hbase_20190115_copy.properties \
    /user/handongqing/fqz_rrd_register_mobile_edges_decompressed \
    /user/handongqing/user_distribution

if [ $? -ne 0 ]
then
    echo "run mapreduce failed !"
    exit 255
fi


