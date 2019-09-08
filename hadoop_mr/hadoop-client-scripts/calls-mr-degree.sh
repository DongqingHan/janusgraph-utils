#!/bin/bash 

MAIN_CLASS="DegreeCounter"
if [ -z "$HADOOP_HOME" ]; then
    export HADOOP=`which hadoop`
else
    export HADOOP="$HADOOP_HOME/bin/hadoop"
fi

basedir=`dirname "${BASH_SOURCE-$0}"`
jardir="/var/lib/hadoop-hdfs/handongqing/libjars"
libdir="/var/lib/hadoop-hdfs/handongqing/libjars"
JAR=$(ls $jardir/anti-fraud*.jar)
dependencies=$(ls $libdir/*.jar | tr '\n' :)
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$dependencies
export LIBJARS=`echo ${dependencies} | sed s/:/,/g`
export JAVA_HOME=/usr/java/jdk

echo "run command : $HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} reducer_number threshold input_path output_path "

hdfs dfs -rm -r -f /user/handongqing/calls_vertex_degree_all
$HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} 100 2000 \
    /user/handongqing/calls_vertex_degree_all \
    /user/handongqing/fqz_call_details_edges \
    /user/handongqing/fqz_phonebook_rrd_edges \
    /user/handongqing/fqz_phonebook_hh_edges

if [ $? -ne 0 ]
then
    echo "run mapreduce failed !"
    exit 255
fi

 
