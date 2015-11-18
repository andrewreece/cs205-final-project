#!/bin/bash
#Run only on master
if sudo grep isMaster /mnt/var/lib/info/instance.json | sudo grep true;
then
kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tweets
fi


