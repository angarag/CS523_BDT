#Visit https://kafka.apache.org/quickstart
#Run ZooKeeper if there is no Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties
#Run Kafka
bin/kafka-server-start.sh config/server.properties
#Create topic election
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic election
#List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
#Consumer
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic election --from-beginning
#Producer with piped message
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic election < message.txt
