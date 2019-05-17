package bdt.mars.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Timestamp;
import java.util.Properties;

public class Producer {
	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
				props);
		String[] candidates = {"Arya","Jon","Sansa","Sersei","Daenerys"};
		for (int i = 0; i < 100; i++) {
			ProducerRecord<String, String> data;
			double random = Math.random()*candidates.length;
			 Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			data = new ProducerRecord<String, String>("election", 0,
					candidates[(int)random], new String(candidates[(int)random]+","+
			timestamp.getTime()
					));
			producer.send(data);
			Thread.sleep(1L);
		}
		producer.close();
	}
}