sudo service hbase-regionserver stop
sudo service hbase-master stop
//sudo fuser -k -n tcp 2181
//sudo fuser -k -n tcp 9090
//sudo fuser -k -n tcp 9092
sudo service hbase-regionserver start
sudo service hbase-master start
//hbase shell
sudo service zookeeper-server start
sudo service zookeeper-server stop

