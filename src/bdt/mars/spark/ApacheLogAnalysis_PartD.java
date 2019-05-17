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

public class ApacheLogAnalysis_PartD {

	public static void main(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"apacheLogAnalysis").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(input);

		// Calculate word count
		JavaPairRDD<String, Integer> IPs = lines
				.map(line -> line.split(" ")[0])
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y).filter(ip -> ip._2 >= 20).cache();
		IPs.collect().forEach(System.out::println);
		IPs.saveAsTextFile(output);

		sc.close();
	}
}
