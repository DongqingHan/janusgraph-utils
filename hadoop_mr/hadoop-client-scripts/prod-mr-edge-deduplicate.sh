#!/bin/bash 

MAIN_CLASS="DeDuplicate"
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

echo "run command : $HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} output_path input_path"

$HADOOP fs -rm -r -f /user/handongqing/prod_deduplicate_result
$HADOOP jar ${JAR} $MAIN_CLASS -libjars ${LIBJARS} 1000 /user/handongqing/prod_deduplicate_result \
    /user/handongqing/fqz_online_first_due_properties \
    /user/handongqing/fqz_blacklist_all \
    /user/handongqing/fqz_is_final_passed_tag \
    /user/handongqing/fqz_ever_loan_tag_all \
    /user/handongqing/fqz_all_strong_edges


if [ $? -ne 0 ]
then
    echo "run hive sql failed !"
    exit 255
fi

 


