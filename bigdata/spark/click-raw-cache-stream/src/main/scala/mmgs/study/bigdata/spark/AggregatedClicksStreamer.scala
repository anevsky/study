package mmgs.study.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AggregatedClicksStreamer {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: AggregatedClicksStreamer <zkQuorum>, <group>, <topics>, <numThreads>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("RawClicksStreamer")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, 1)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, "my-consumer-group", topicMap).map(_._2)

    val rawClicks = lines.map(l => l.split("\t"))
      .map(i => (i(0) //bidId
        , i(1) //timestamp
        , i(2) //ipinyouId
        , i(3) //user-agent
        , i(4) //ip
        , i(5) //region
        , i(6) //city
        , i(16) //payingPrice
        , i(18) //biddingPrice
        , i(21) //streamId
        , i(20) //tags
        ))

    rawClicks.foreachRDD { rdd =>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val clicksDataFrame = rdd.toDF("bidId", "timestamp", "ipinyouId", "userAgent", "ip", "region", "city", "payingPrice", "biddingPrice", "streamId", "tags")

      /*
      •	Joined with all the dictionaries raw data
•	Session behavior, one record for session with all clicked, searched, bought tags in a TOKEN_MAP(KEYWORD_1, AMOUNT_1... KEYWORD_N, AMOUNT_N).

       */


      // Create a temporary view
      clicksDataFrame.createOrReplaceTempView("words")

      // Do word count on DataFrame using SQL and print it
      val wordCountsDataFrame =
      spark.sql("select word, count(*) as total from words group by word")
      wordCountsDataFrame.show()
    }

    //somethingFinal.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
