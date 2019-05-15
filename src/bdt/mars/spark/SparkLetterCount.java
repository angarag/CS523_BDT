package bdt.mars.spark;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.primitives.Chars;

import scala.Tuple2;

public class SparkLetterCount {

	public static void main(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		int limit = Integer.parseInt(args[2]);
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"wordCount").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(input);

		// Calculate letter count
		JavaPairRDD<String, Integer> letters = lines
				.flatMap(line -> Arrays.asList(line.split(" ")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y)
				.mapToPair(w -> new Tuple2<Integer, String>(w._2, w._1))
				.filter(w -> w._1 >= limit)
				.flatMap(word -> Chars.asList(word._2.toCharArray()))
				.mapToPair(c -> new Tuple2<String, Integer>(c.toString(), 1))
				.reduceByKey((x, y) -> x + y)
				.mapToPair(w -> new Tuple2<Integer, String>(w._2, w._1))
				.sortByKey(false)
				.mapToPair(w -> new Tuple2<String, Integer>(w._2, w._1))
				.cache();
		System.out
				.println("Printing the letters with their occurences in descending order");
		letters.collect().forEach(System.out::println);
		// Save the letter count back out to a text file, causing evaluation
		letters.saveAsTextFile(output);

		sc.close();
	}
}
