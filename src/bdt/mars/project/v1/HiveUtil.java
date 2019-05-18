package bdt.mars.project.v1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class HiveUtil {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		// warehouseLocation points to the default location for managed
		// databases and tables
		SparkSession spark = SparkSession.builder()
				.appName("Java Spark Hive Example")
				.config("spark.master", "local").enableHiveSupport()
				.getOrCreate();

		spark.sql("CREATE TABLE IF NOT EXISTS vote (candidate STRING, user STRING, date TIMESTAMP) USING hive");
		spark.sql("LOAD DATA LOCAL INPATH 'input/election_votes.txt' OVERWRITE INTO TABLE vote");
		// spark.sql("LOAD DATA INPATH 'user/cloudera/input/election_votes.txt' OVERWRITE INTO TABLE vote");
		// spark.sql("SELECT candidate,COUNT(*) FROM vote GROUP BY candidate").show();

	}
}