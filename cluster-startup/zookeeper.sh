#!/bin/bash

#Run only on master
if sudo grep isMaster /mnt/var/lib/info/instance.json | sudo grep true;
then
ZOOKEEPERV=3.4.6
wget  --no-check-certificate http://apache.mirrors.tds.net/zookeeper/stable/zookeeper-${ZOOKEEPERV}.tar.gz
tar vxzf zookeeper*tar.gz
cd zookeeper-$ZOOKEEPERV
mv conf/zoo_sample.cfg conf/zoo.cfg
sudo bin/zkServer.sh start
fi

