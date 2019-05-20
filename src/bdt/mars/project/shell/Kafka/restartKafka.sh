sudo fuser -k -n tcp 9092
cd /home/cloudera/Downloads/kafka_2.12-2.2.0
bin/kafka-server-start.sh config/server.properties

