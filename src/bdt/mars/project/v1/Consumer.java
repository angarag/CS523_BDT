package bdt.mars.project.v1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import scala.Tuple2;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Consumer {
	static String fileName = "input/election_votes.txt";
	static File file = new File(fileName);

	public static void main(String args[]) throws InterruptedException, FileNotFoundException, IOException {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		HTTPPostSender.init();
		
		Collection<String> topics = Arrays.asList("test", "election");
		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster(
				"local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(
				500));
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
				.createDirectStream(ssc, LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String> Subscribe(topics,
								kafkaParams));
		String[] vote = new String[3];
		stream.mapToPair(record -> {
			vote[0] = record.key();
			vote[1] = record.value().split(",")[0];
			vote[2] = record.value().split(",")[1];
			helper(vote);
			return new Tuple2<>(record.key(), 1);
		})
				.reduceByKeyAndWindow((i1, i2) -> i1 + i2,
						new Duration(1000 * 60 * 5), new Duration(500)).print();
		ssc.start(); // Start the computation
		ssc.awaitTermination(); // Wait for the computation to terminate
	}

	public static void helper(String[] vote_record) throws IOException {
		// System.out.println(Arrays.toString(vote_record));
		String candidate = vote_record[0];
		String user = vote_record[1];
		String timestamp = vote_record[2];
		// save into ElasticSearch
		String[] args = { user, candidate, "1", timestamp };
		HTTPPostSender.saveToES(args);
		// append into input/election_votes.txt
		String row = candidate + "" + user + "" + timestamp + "\n";
		FileUtils.writeStringToFile(file, row, StandardCharsets.UTF_8, true);
	}
}