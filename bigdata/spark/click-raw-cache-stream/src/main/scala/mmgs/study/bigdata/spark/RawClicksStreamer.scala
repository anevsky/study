package mmgs.study.bigdata.spark

import it.nerdammer.spark.hbase._
import it.nerdammer.spark.hbase.conversion.{FieldWriter, FieldWriterProxy}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

    val clicks = lines.map(l => l.split("\t"))
      .map(i => (i(2) + i(1) //ipinyouId + timestamp
        , new ClickInfo(i(0) //bidId
        , i(1) //timestamp
        , i(2) //ipinyouId
        , i(3) //user-agent
        , i(4) //ip
        , i(5) //region
        , i(6) //city
        , i(16) //payingPrice
        , i(18) //biddingPrice
        , i(21) //streamId
        , i(20)) //userTags
        , new ClickAdInfo(i(7) //adExchange
        , i(8) //domain
        , i(9) //url
        , i(10) //anonimousUrlId
        , i(11) //adSlotId
        , i(12) //adSlotWidth
        , i(13) //adSlotHeight
        , i(14) //adSlotVisibility
        , i(15) //adSlotFormat
        , i(17) //creativeId
        , i(19)) //advertiserId
        ))

    clicks.foreachRDD(rdd =>
      rdd.toHBaseTable("click.raw")
        .inColumnFamily("click")
        .toColumns("bid"
          , "time"
          , "ipyId"
          , "userAgent"
          , "ip"
          , "region"
          , "city"
          , "payPrice"
          , "bidPrice"
          , "stream"
          , "tags"
          , "ad:adExch"
          , "ad:domain"
          , "ad:url"
          , "ad:anonUrl"
          , "ad:adSlot"
          , "ad:adSlotW"
          , "ad:adSlotH"
          , "ad:adSlotV"
          , "ad:adSlotF"
          , "ad:creativeId"
          , "ad:adId")
        .save()
    )

    ssc.start()
    ssc.awaitTermination()
  }

  implicit def clickInfoWriter: FieldWriter[ClickInfo] = new FieldWriterProxy[ClickInfo, (String, String, String, String, String, String, String, String, String, String, String)] {
    override def convert(data: ClickInfo) = (data.bidId
      , data.timestamp
      , data.ipinyouId
      , data.userAgent
      , data.ip
      , data.region
      , data.city
      , data.payingPrice
      , data.biddingPrice
      , data.streamId
      , data.userTags)
  }

  implicit def clickAdInfoWriter: FieldWriter[ClickAdInfo] = new FieldWriterProxy[ClickAdInfo, (String, String, String, String, String, String, String, String, String, String, String)] {
    override def convert(data: ClickAdInfo) = (data.adExchange,
      data.domain,
      data.url,
      data.anonimousUrlId,
      data.adSlotId,
      data.adSlotWidth,
      data.adSlotHeight,
      data.adSlotVisibility,
      data.adSlotFormat,
      data.creativeId,
      data.advertiserId)
  }
}
