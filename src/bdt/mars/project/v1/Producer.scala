package bdt.mars.project
import java.util.Properties
import org.apache.kafka.clients.producer._

object Producer extends App {
  val props = new Properties()
  props.put("bootstrap.servers", "kafka-66012d1-mum-6eb6.aivencloud.com:23237")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC = "election"

  for (i <- 1 to 50) {
    val record = new ProducerRecord(TOPIC, "key", s"hello $i")
    producer.send(record)
  }

  val record = new ProducerRecord(TOPIC, "key", "the end " + new java.util.Date)
  producer.send(record)

  producer.close()
  print("producer done")

}