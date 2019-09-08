#!/bin/bash 

MAIN_CLASS="PartAdjacentList"
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

# 16G java heap memory
export HADOOP_OPTS="-Xmx16384m"
echo "run command : $HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} reducer_number input_path output_path "

hdfs dfs -rm -r -f /user/handongqing/calls_filtered_one_adjacent
$HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} \
    -D mapreduce.reduce.shuffle.parallelcopies=2 \
    -D mapreduce.map.java.opts="-Xmx3500m" \
    -D mapreduce.reduce.java.opts="-Xmx14000m" \
    500 \
    /user/handongqing/calls_filtered_deduplicate_result \
    /user/handongqing/calls_filtered_one_adjacent

if [ $? -ne 0 ]
then
    echo "run hive sql failed !"
    exit 255
fi

 
