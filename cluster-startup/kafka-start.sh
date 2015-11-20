#!/bin/bash

#Run only on master
if sudo grep isMaster /mnt/var/lib/info/instance.json | sudo grep true;
then
#get things started!
screen
sudo /home/hadoop/kafka/bin/kafka-server-start.sh /home/hadoop/kafka/config/server.properties
fi

