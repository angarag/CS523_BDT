package bdt.mars.spark.project.kafka.stream
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

object NetworkCount {
  def setupLogging() = {
    import org.apache.log4j.{ Level, Logger }
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
  // Create a local StreamingContext with two working thread and batch interval of 1 second.
  // The master requires 2 cores to prevent a starvation scenario.
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    setupLogging()
    val lines = ssc.socketTextStream("localhost", 8080)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}