#!/bin/bash

#install kafka
sudo wget http://apache.go-parts.com/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz
sudo tar xzf kafka_2.10-0.8.2.0.tgz
sudo mkdir logs
aws s3 cp s3://cs205-final-project/setup/kafka/server.properties.aws kafka/config/server.properties

#get things started!
bin/kafka-server-start.sh config/server.properties

