#!/bin/bash

# switch to python 2.7.9
sudo alternatives --set python /usr/bin/python2.7
# upgrade pip and install boto3, awscli
# note: there was some funky issue with boto3 when you didn't upgrade pip, that's why the pip upgrade.
sudo easy_install --upgrade pip
>&2 echo "check1"
sudo `which pip` install --upgrade requests
>&2 echo "check2"
sudo `which pip` install boto3 requests_oauthlib kafka-python findspark python-dateutil

mkdir /home/hadoop/scripts
mkdir /home/hadoop/.aws 

aws s3 cp s3://cs205-final-project/setup/kafka/spark-streaming-kafka-assembly_2.10-1.5.2.jar /home/hadoop/
aws s3 cp s3://cs205-final-project/scripts /home/hadoop/scripts/ --recursive
aws s3 cp s3://cs205-final-project/setup/aws/credentials /home/hadoop/.aws/credentials

sudo chmod -R 744 /home/hadoop/scripts/*
sudo chmod 644 /home/hadoop/.aws/credentials


