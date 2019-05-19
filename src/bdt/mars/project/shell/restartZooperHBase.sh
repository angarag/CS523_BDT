sudo service zookeeper-server stop
sudo service zookeeper-server start
sudo service hbase-regionserver stop
sudo service hbase-regionserver start
sudo service hbase-master stop
sudo service hbase-master start
//hbase shell
//sudo fuser -k -n tcp 2181
//sudo fuser -k -n tcp 9090
//sudo fuser -k -n tcp 9092


