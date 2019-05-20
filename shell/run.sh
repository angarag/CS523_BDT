#Run Kafka
cd ~/Downloads/kafka_2.12-2.2.0
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic election
#Create election table in HBase
hbase shell << create 'election', 'vote_details'
#Create Hive-Hbase mapping
hive << CREATE EXTERNAL TABLE election(id STRING, voteFor STRING, user STRING, count STRING, date TIMESTAMP) ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe' STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES("hbase.columns.mapping" = ":key,vote_details:voteFor,vote_details:user,vote_details:count,vote_details:timestamp") TBLPROPERTIES("hbase.table.name"="election");
#After exporting it as a runnable jar, run the followings (it is assumed that the jar file is exported to Desktop folder):
cd ~/Desktop
spark-submit --class bdt.mars.project.v1.Producer --master yarn CS523_986689.jar
sleep 3s
mkdir input
mkdir conf
touch conf/app.properties
URL=$Elastic_URL
PASSWORD=$Elastic_password
echo "url=$URL" >> conf/app.properties
echo "password=$PASSWORD" >> conf/app.properties
spark-submit --class bdt.mars.project.v1.Consumer --master yarn CS523_986689.jar
sleep 300s
spark-submit --class bdt.mars.project.v1.HiveUtil --master yarn CS523_986689.jar