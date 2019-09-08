#!/bin/bash 

MAIN_CLASS="PartAdjacentList"
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
# set 16G heap set for Java to handle the error Error: Java heap space
# export HADOOP_OPTS="-Xmx16384m"
echo "run command : $HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} input_path output_path "

$HADOOP fs -rm -r -f /user/handongqing/prod_part_adjacent/*
# add map/reduce jvm heap maximum size with `-D mapreduce.map.java.opts="-Xmx4000m" -D mapreduce.reduce.java.opts="-Xmx6500m" `
$HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} 1000 /user/handongqing/prod_deduplicate_result /user/handongqing/prod_one_adjacent

if [ $? -ne 0 ]
then
    echo "run hive sql failed !"
    exit 255
fi

 
