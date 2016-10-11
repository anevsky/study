package mmgs.study.bigdata.spark

import it.nerdammer.spark.hbase._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object RawClicksStreamer {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: RawClicksStreamer <zkQuorum>, <group>, <topics>, <numThreads>")
      System.exit(1)
    }

    //TODO: WAL, checkpointing etc.
    val sparkConf = new SparkConf().setAppName("RawClicksStreamer")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, 1)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, "my-consumer-group", topicMap).map(_._2)

    val clicks = lines.map(l => l.split("\t")).map(i => (i(1) + i(2) + i(0) //timestamp + ipinyouId + bidId
      , i(0) //bidId
      , i(1) //timestamp
      , i(2) //ipinyouId
      , i(3) //user-agent
      , i(4) //ip
      , i(5) //region
      , i(6) //city
      , i(16) //payingPrice
      , i(18) //biddingPrice
      , i(21) //streamId

      , i(7) //adExchange
      , i(8) //domain
      , i(9) //url
      , i(10) //anonimousUrlId
      , i(11) //adSlotId
      , i(12) //adSlotWidth
      , i(13) //adSlotHeight
      , i(14) //adSlotVisibility
      , i(15) //adSlotFormat
      , i(17) //creativeId
      , i(19) //advertiserId

      , i(20) //userTags
      ))

    clicks.foreachRDD(rdd =>
      rdd.toHBaseTable("click.raw")
        .inColumnFamily("value")
        .toColumns("value")
        .save()
    )

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
