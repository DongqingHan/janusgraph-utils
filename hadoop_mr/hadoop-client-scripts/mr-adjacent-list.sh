#!/bin/bash 

MAIN_CLASS="AdjacentList"
if [ -z "$HADOOP_HOME" ]; then
    export HADOOP=`which hadoop`
else
    export HADOOP="$HADOOP_HOME/bin/hadoop"
fi

basedir=`dirname "${BASH_SOURCE-$0}"`
jardir="/home/dev/handongqing/libjars"
libdir="/home/dev/handongqing/libjars"
JAR=$(ls $jardir/babel*.jar)
dependencies=$(ls $libdir/*.jar | tr '\n' :)
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$dependencies
export LIBJARS=`echo ${dependencies} | sed s/:/,/g`
export JAVA_HOME=/usr/java/jdk


echo "run command : $HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} input_path output_path "
$HADOOOP fs -rm -r -r /user/handongqing/whole_graph
$HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} /user/handongqing/zyc_janusgraph_data/ /user/handongqing/whole_graph


if [ $? -ne 0 ]
then
    echo "run hive sql failed !"
    exit 255
fi

 


