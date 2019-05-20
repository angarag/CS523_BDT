#Run Kafka
cd ~/Downloads/kafka_2.12-2.2.0
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic election
