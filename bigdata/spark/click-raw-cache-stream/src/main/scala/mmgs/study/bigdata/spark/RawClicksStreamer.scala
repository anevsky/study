package mmgs.study.bigdata.spark

import it.nerdammer.spark.hbase._
import it.nerdammer.spark.hbase.conversion.{FieldWriter, FieldWriterProxy}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RawClicksStreamer {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: RawClicksStreamer <zkQuorum>, <group>, <topics>, <numThreads>, <checkpointDir>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("RawClicksStreamer")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[ClickInfo], classOf[ClickAdInfo]))

    val Array(zkQuorum, group, topics, numThreads, checkpointDir) = args

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(checkpointDir)

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val clicks = lines.map(l => l.split("\t")).filter(i => !i(2).equals("null"))
      .map(i => ((i(2) + i(1)).hashCode //ipinyouId + timestamp
        , ClickInfo(i(0) //bidId
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
        , ClickAdInfo(i(7) //adExchange
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

    clicks.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.toHBaseTable("click_raw")
          .inColumnFamily("c")
          .toColumns("bid"
            , "ts"
            , "ipyId"
            , "userAgent"
            , "ip"
            , "region"
            , "city"
            , "payP"
            , "bidP"
            , "log"
            , "tags"
            , "ad:exch"
            , "ad:domain"
            , "ad:url"
            , "ad:anonUrl"
            , "ad:slot"
            , "ad:slotW"
            , "ad:slotH"
            , "ad:slotV"
            , "ad:slotF"
            , "ad:creativeId"
            , "ad:id")
          .save()
      }
    }

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
      , data.logType
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
