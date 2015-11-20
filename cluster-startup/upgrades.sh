#!/bin/bash

# switch to python 2.7.9
sudo alternatives --set python /usr/bin/python2.7
# upgrade pip and install boto3, awscli
# note: there was some funky issue with boto3 when you didn't upgrade pip, that's why the pip upgrade.
sudo easy_install --upgrade pip
sudo `which pip` install boto3 awscli requests requests_oauthlib kafka-python

mkdir /home/hadoop/scripts
mkdir /home/hadoop/startup

aws s3 cp s3://cs205-final-project/setup/kafka/spark-streaming-kafka-assembly_2.10-1.5.2.jar /home/hadoop/
aws s3 cp s3://cs205-final-project/scripts /home/hadoop/scripts/ --recursive
aws s3 cp s3://cs205-final-project/setup/startup/ /home/hadoop/startup/ --recursive

sudo chmod 777 /home/hadoop/startup/kafka-start.sh
sudo chmod 777 /home/hadoop/startup/kafka-topic.sh