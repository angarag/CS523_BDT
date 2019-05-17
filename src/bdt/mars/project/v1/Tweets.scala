package bdt.mars.spark.project.kafka.stream;

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

/**
 * Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 *
 *
 * Spark Dstream:
 * https://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams
 * Spark Twitter:
 * https://bahir.apache.org/docs/spark/2.0.0/spark-streaming-twitter/
 * Spark Kafka integration:
 * https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
 *
 */
object Tweets {

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{ Level, Logger }
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source

    for (line <- Source.fromFile("../twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "MUM tweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "MUM tweets", Seconds(3))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    val mum = tweets //.filter(tweet => tweet.getText().contains("testMUMtweets"))
      .map(x => (x, 1)).reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(3))

    mum.print

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("/home/angarag/checkpoint")
    ssc.start()
    print("awaiting termination")
    //    ssc.stop(true)
    ssc.awaitTermination()
  }
}
