#!/bin/bash
# Description: Export hadoop-common jar to Hadoop installations across the cluster

ip_list=(
    rh8-50
    rh8-51
    rh8-52
    rh8-53
    rh8-54
    rh8-55
)

SOURCE_JAR=./hadoop-common/target/hadoop-common-3.4.0.jar
HAODOP_HOME=/home/hadoop/hadoop_installations/hadoop-3.4.0
for ip in ${ip_list[*]}; do
    scp $SOURCE_JAR hadoop@$ip:$HAODOP_HOME/share/hadoop/common/hadoop-common-3.4.0.jar
done