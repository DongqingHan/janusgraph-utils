#!/bin/bash 

MAIN_CLASS="FilterDeduplicate"
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

echo "run command : $HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} reducer_number big_vertex_path output_path input_path input_path ..."

hdfs dfs -rm -r -f /user/handongqing/calls_filtered_deduplicate_result
$HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} \
    -D mapreduce.output.fileoutputformat.compress="true" \
    -D mapreduce.output.fileoutputformat.compress.codec="org.apache.hadoop.io.compress.SnappyCodec" \
    -D mapreduce.map.java.opts="-Xmx3500m" \
    -D mapreduce.reduce.java.opts="-Xmx14000m" \
    500 \
    /user/handongqing/calls_vertex_degree_all \
    /user/handongqing/calls_filtered_deduplicate_result \
    /user/handongqing/fqz_online_first_due_properties \
    /user/handongqing/fqz_blacklist_all \
    /user/handongqing/fqz_is_final_passed_tag \
    /user/handongqing/fqz_ever_loan_tag_all \
    /user/handongqing/fqz_all_strong_edges \
    /user/handongqing/fqz_call_details_edges \
    /user/handongqing/fqz_callrec_added \
    /user/handongqing/fqz_phonebook_rrd_edges \
    /user/handongqing/fqz_phonebook_rrd_added \
    /user/handongqing/fqz_phonebook_hh_edges \
    /user/handongqing/fqz_phonebook_hh_added

if [ $? -ne 0 ]
then
    echo "run hive sql failed !"
    exit 255
fi

