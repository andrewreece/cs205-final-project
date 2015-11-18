#!/bin/bash

yum upgrade

# switch to python 2.7.9
sudo alternatives --set python /usr/bin/python2.7
# upgrade pip and install boto3, awscli
# note: there was some funky issue with boto3 when you didn't upgrade pip, that's why the pip upgrade.
sudo easy_install --upgrade pip
sudo `which pip` install boto3
sudo `which pip` install awscli


HOMEDIR=/home/hadoop
ZOOKEEPERV=3.4.6
ZOODIR="zookeeper-3.4.6"
ZOO_LOG_DIR=/mnt/var/log/zookeeper

#Installing Zookeeper
#echo "Downloading Zookeeper"
wget http://apache.mirrors.tds.net/zookeeper/stable/zookeeper-${ZOOKEEPERV}.tar.gz
tar xzf zookeeper*tar.gz

#Run only on master
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
mkdir -p ${ZOO_LOG_DIR}/log
mkdir -p ${ZOO_LOG_DIR}/data
echo "clientPort=2181" > $HOMEDIR/$ZOODIR/conf/zoo.cfg
echo "dataDir=${ZOO_LOG_DIR}/snapshot" >> $HOMEDIR/$ZOODIR/conf/zoo.cfg
echo "syncLimit=5" >> $HOMEDIR/$ZOODIR/conf/zoo.cfg
echo "tickTime=2000" >> $HOMEDIR/$ZOODIR/conf/zoo.cfg
echo "initLimit=10" >> $HOMEDIR/$ZOODIR/conf/zoo.cfg
echo "maxClientCnxns=100" >> $HOMEDIR/$ZOODIR/conf/zoo.cfg
echo "dataLogDir=${ZOO_LOG_DIR}/data" >> $HOMEDIR/$ZOODIR/conf/zoo.cfg
#echo "Starting Zookeeper..."
export ZOO_LOG_DIR=${ZOO_LOG_DIR}
$HOMEDIR/$ZOODIR/bin/zkServer.sh start
fi

sudo rm $ZOODIR.tar.gz

#install kafka
sudo wget http://apache.go-parts.com/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz
sudo tar xzf kafka_2.10-0.8.2.0.tgz
sudo mv kafka_2.10-0.8.2.0 kafka
sudo mkdir logs
aws s3 cp s3://cs205-final-project/setup/kafka/server.properties.aws kafka/config/server.properties

#get things started!
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tweets

