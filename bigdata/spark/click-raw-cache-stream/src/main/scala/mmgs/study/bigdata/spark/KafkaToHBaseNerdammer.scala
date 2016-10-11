package mmgs.study.bigdata.spark

import it.nerdammer.spark.hbase._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaToHBaseNerdammer {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <broker> <topic>")
      System.exit(1)
    }

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
      //.setMaster("local[*]").set("spark.hbase.host", "quickstart.cloudera")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
//    ssc.checkpoint("checkpoint")

    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, 1)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, "my-consumer-group", topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      //.reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)

/*    val Array(brokers, topics) = args
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)*/


    wordCounts.foreachRDD(rdd =>
      rdd.toHBaseTable("simpleTable")
        .inColumnFamily("value")
        .toColumns("value")
        .save()
    )

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}