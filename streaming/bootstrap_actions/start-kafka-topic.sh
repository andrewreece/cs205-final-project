#!/bin/bash

LOCALIP=$(curl http://169.254.169.254/latest/meta-data/local-ipv4)
/home/hadoop/kafka/bin/kafka-topics.sh --create --zookeeper $LOCALIP:2181 --replication-factor 1 --partitions 1 --topic tweets

