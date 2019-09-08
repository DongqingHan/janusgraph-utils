#!/bin/bash 

MAIN_CLASS="DataSupplement"
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

$HADOOP jar ${JAR} $MAIN_CLASS \
  -files hdfs://nameservice1/user/handongqing/prod_hbase_callrec_20181221_bak.properties#prod_hbase_callrec_20181221_bak.properties \
  -libjars ${LIBJARS} \
  prod_hbase_callrec_20181221_bak.properties \
  /user/handongqing/fqz_call_details_edges_20181210_20190117/ \
  /user/handongqing/tmp

if [ $? -ne 0 ]
then
    echo "run mapreduce failed !"
    exit 255
fi

 
