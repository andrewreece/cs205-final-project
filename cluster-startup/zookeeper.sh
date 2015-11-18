#!/bin/bash

HOMEDIR=/home/hadoop
ZOOKEEPERV=3.4.6
ZOODIR="zookeeper-3.4.6"
ZOO_LOG_DIR=/mnt/var/log/zookeeper

#Installing Zookeeper
#echo "Downloading Zookeeper"
wget http://apache.mirrors.tds.net/zookeeper/stable/zookeeper-${ZOOKEEPERV}.tar.gz
tar xzf zookeeper*tar.gz
sudo rm $ZOODIR.tar.gz

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
#start zookeeper
$HOMEDIR/$ZOODIR/bin/zkServer.sh start
fi

