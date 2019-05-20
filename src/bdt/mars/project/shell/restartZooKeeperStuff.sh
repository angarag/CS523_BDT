#Restart ZooKeeper
sudo service zookeeper-server stop
sudo service zookeeper-server start

#Restart HBase
sudo service hbase-regionserver stop
sudo service hbase-regionserver start
sudo service hbase-master stop
sudo service hbase-master start

#Stop Kafka
sudo fuser -k -n tcp 9092

#Kill processes by port (ZooKeeper:2181, Kafka: 9092)
#sudo fuser -k -n tcp 9092


