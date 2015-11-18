#!/bin/bash

#Run only on master
if sudo grep isMaster /mnt/var/lib/info/instance.json | sudo grep true;
then
#get things started!
sudo kafka/bin/kafka-server-start.sh kafka/config/server.properties
fi

