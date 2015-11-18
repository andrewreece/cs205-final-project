#!/bin/bash

#install kafka
sudo wget http://apache.go-parts.com/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz
sudo tar xzf kafka_2.10-0.8.2.0.tgz
sudo mv kafka_2.10-0.8.2.0 kafka 
sudo mkdir logs
sudo chmod 755 kafka/config/server.properties
sudo aws s3 cp s3://cs205-final-project/setup/kafka/server.properties.aws kafka/config/server.properties

#get things started!
sudo kafka/bin/kafka-server-start.sh kafka/config/server.properties

