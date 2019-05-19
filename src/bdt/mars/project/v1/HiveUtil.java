package bdt.mars.project.v1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class HiveUtil {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		// SparkConf conf = new
		// SparkConf().setAppName("kafka-sandbox").setMaster(
		// "local[*]");
		// JavaSparkContext sc = new JavaSparkContext(conf);
		// SQLContext scontext = new SQLContext(sc);
		// Dataset<Row> ds =
		// scontext.sql("SELECT COUNT(*) FROM election");//voteFor,COUNT(*) FROM
		// election GROUP BY voteFor");
		// ds.show();
		// warehouseLocation points to the default location for managed
		// databases and tables
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark Hive Example")
				.master("local[*]")
				.config("spark.sql.warehouse.dir",
						"hdfs://quickstart.cloudera:8020/user/hive/warehouse")
				.enableHiveSupport().getOrCreate();
		/*
		 * spark.sql(
		 * "CREATE TABLE IF NOT EXISTS vote (voteFor STRING, user STRING, date TIMESTAMP, count INT) USING hive"
		 * ); spark.sql(
		 * "LOAD DATA LOCAL INPATH 'input/election_votes.txt' OVERWRITE INTO TABLE vote"
		 * );
		 * 
		 * String s =
		 * "create external table election (id STRING, voteFor STRING, user STRING, count STRING, date TIMESTAMP) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES(\"hbase.columns.mapping\" = \":key,vote_details:voteFor,vote_details:user,vote_details:count,vote_details:timestamp\") TBLPROPERTIES(\"hbase.table.name\"=\"election\")"
		 * ; spark.sql(s); spark.sql(
		 * "LOAD DATA INPATH 'user/cloudera/input/election_votes.txt' OVERWRITE INTO TABLE vote"
		 * );
		 */
		spark.sql("SELECT COUNT(*) FROM election").show();
		spark.sql("SELECT voteFor,COUNT(*) FROM election GROUP BY voteFor").show();
	}
}