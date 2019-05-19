package bdt.mars.project.v1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import scala.Tuple2;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Consumer {
	static String fileName = "input/election_votes.txt";
	static File file = new File(fileName);

	static void init() throws IOException {
		PrintWriter writer = new PrintWriter(file);
		writer.print("");
		writer.close();
		ElasticSearchUtil.init();
		HBaseUtil.init();
	}

	public static void main(String args[]) throws InterruptedException,
			FileNotFoundException, IOException {
		init();
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

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
		String[] vote = new String[5];
		JavaPairDStream<String, Integer> vote_stream = stream.mapToPair(
				record -> {
					vote[0] = record.key();// voteFor
					vote[1] = record.value().split(",")[0];// user
					vote[2] = record.value().split(",")[1];// timestamp
					vote[3] = vote[0] + "," + vote[1];
					vote[4] = "1";
					helper(vote);
					return new Tuple2<>(record.key(), 1);
				}).cache();
		// vote_stream.saveAsHadoopFiles("/home/cloudera", "kafka_output");
		vote_stream.reduceByKeyAndWindow((i1, i2) -> i1 + i2,
				new Duration(1000 * 60 * 5), new Duration(500)).print();
		ssc.start(); // Start the computation
		ssc.awaitTermination(); // Wait for the computation to terminate
	}

	public static void helper(String[] vote_record) throws IOException {
		// System.out.println(Arrays.toString(vote_record));
		// save into ElasticSearch
		ElasticSearchUtil.saveToES(vote_record);
		// save into HBase election table
		HBaseUtil.saveRecord(vote_record);
		// append into input/election_votes.txt
		String row = vote_record[0] + "" + vote_record[1] + ""
				+ vote_record[2] + "\n";

		FileUtils.writeStringToFile(file, row, StandardCharsets.UTF_8, true);
	}
}