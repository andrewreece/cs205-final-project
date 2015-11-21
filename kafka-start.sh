#!/bin/bash

#Run only on master
if sudo grep isMaster /mnt/var/lib/info/instance.json | sudo grep true;
then
#get things started!
(sudo /home/hadoop/kafka/bin/kafka-server-start.sh /home/hadoop/kafka/config/server.properties &)
/home/hadoop/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tweets
fi

