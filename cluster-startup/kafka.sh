#!/bin/bash

#Run only on master
if sudo grep isMaster /mnt/var/lib/info/instance.json | sudo grep true;
then
KAFKAV=kafka_2.10-0.8.2.0
#install kafka
sudo wget http://apache.go-parts.com/kafka/0.8.2.0/$KAFKAV.tgz
tar xzf $KAFKAV.tgz
sudo mv $KAFKAV kafka 
mkdir logs
sudo chmod 755 kafka/config/server.properties
aws s3 cp s3://cs205-final-project/setup/kafka/server.properties.aws kafka/config/server.properties
fi


