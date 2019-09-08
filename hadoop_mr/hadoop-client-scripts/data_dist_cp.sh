#!/bin/bash

hdfs dfs -rm -r /user/handongqing/fqz_online_first_due_properties
hdfs dfs -rm -r /user/handongqing/fqz_blacklist_all
hdfs dfs -rm -r /user/handongqing/fqz_is_final_passed_tag
hdfs dfs -rm -r /user/handongqing/fqz_ever_loan_tag_all
hdfs dfs -rm -r /user/handongqing/fqz_all_strong_edges
hdfs dfs -rm -r /user/handongqing/fqz_callrec_added
hdfs dfs -rm -r /user/handongqing/fqz_call_details_edges

# source namenode ip address may be 10.10.15.60 or 10.10.15.64
hadoop distcp -bandwidth 2 hdfs://10.10.15.60:8020/user/hive/warehouse/tmp.db/fqz_online_first_due_properties /user/handongqing/fqz_online_first_due_properties
hadoop distcp -bandwidth 2 hdfs://10.10.15.60:8020/user/hive/warehouse/tmp.db/fqz_blacklist_all /user/handongqing/fqz_blacklist_all
hadoop distcp -bandwidth 2 hdfs://10.10.15.60:8020/user/hive/warehouse/tmp.db/fqz_is_final_passed_tag /user/handongqing/fqz_is_final_passed_tag
hadoop distcp -bandwidth 2 hdfs://10.10.15.60:8020/user/hive/warehouse/tmp.db/fqz_ever_loan_tag_all /user/handongqing/fqz_ever_loan_tag_all
hadoop distcp -bandwidth 2 hdfs://10.10.15.60:8020/user/hive/warehouse/tmp.db/fqz_all_strong_edges /user/handongqing/fqz_all_strong_edges
hadoop distcp -bandwidth 2 hdfs://10.10.15.60:8020/user/hive/warehouse/tmp.db/fqz_callrec_added /user/handongqing/fqz_callrec_added
hadoop distcp -m 20 -bandwidth 2 hdfs://10.10.15.60:8020/user/hive/warehouse/tmp.db/fqz_call_details_edges/ /user/handongqing/fqz_call_details_edges

