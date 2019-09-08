#!/bin/bash 

MAIN_CLASS="ParallelUpdate"
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

echo "run command : $HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} graph_configuration_file input_path output_path"

$HADOOP jar ${JAR} $MAIN_CLASS -files hdfs://nameservice1/user/handongqing/offline_hbase_20181221_new.properties#offline_hbase_20181221_new.properties -libjars ${LIBJARS} offline_hbase_20181221_new.properties /user/handongqing/fqz_blacklist_mobile/ /user/handongqing/tmp

if [ $? -ne 0 ]
then
    echo "run mapreduce failed !"
    exit 255
fi

 
