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

public class ApacheLogAnalysis_PartC {

	public static void main(String[] args) throws Exception {
		String input = args[0];
		int limit = Integer.parseInt(args[1]);
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"apacheLogAnalysis").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(input);

		// Calculate word count
		List<String> IPs = lines.map(line -> {
			String[] arr = line.split(" ");
			System.out.println(arr[arr.length-2]);
			if (arr.length > 1)
				return arr[arr.length - 2];
			else
				return "";
		}).take(limit);
		long count = sc.parallelize(IPs)
				.filter(status_code -> status_code.equals("401"))
				.count();
		System.out.printf("There are %s 401 errors in the range %s \n", count,
				limit);
		sc.close();
	}
}
