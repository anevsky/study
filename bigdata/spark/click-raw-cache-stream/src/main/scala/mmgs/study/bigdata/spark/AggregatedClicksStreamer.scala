package mmgs.study.bigdata.spark

import eu.bitwalker.useragentutils.UserAgent
import it.nerdammer.spark.hbase._
import it.nerdammer.spark.hbase.conversion.{FieldWriter, FieldWriterProxy}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

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

    val lines: DStream[String] = KafkaUtils.createStream(ssc, zkQuorum, "my-consumer-group", topicMap).map(_._2)

    val rawClicks = lines.map(l => l.split("\t")).filter(i => !i(2).equals("null"))
      .map(i => ClickInfo(i(0) //bidId
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
      if (!rdd.isEmpty()) {
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
        val clicksDF = rdd.toDF

        val cities = Cities.getInstance(rdd.sparkContext)
        val states = States.getInstance(rdd.sparkContext)
        val logTypes = LogType.getInstance(rdd.sparkContext)
        val tags = Tags.getInstance(rdd.sparkContext)

        val citiesDF = cities.value.map(i => (i._1, i._2, i._3)).toDF("id", "city", "stateId")
        val statesDF = states.value.map(i => (i._1, i._2)).toDF("id", "state")
        val logTypeDF = logTypes.value.map(i => (i._1, i._2)).toDF("id", "logType")
        val tagsDF = tags.value.map(i => (i._1, i._2)).toDF("id", "tags").explode("tags", "tag")((tags: String) => tags.split(",")).select("id", "tag").distinct()

        val clicksFullDF = clicksDF.join(citiesDF, clicksDF("city") === citiesDF("id"))
          .join(statesDF, citiesDF("stateId") === statesDF("id"))
          .join(logTypeDF, clicksDF("logType") === logTypeDF("id"))
          .join(tagsDF, clicksDF("userTags") === tagsDF("id"))
          .select(clicksDF("bidId"), clicksDF("timestamp"), clicksDF("ipinyouId"), clicksDF("userAgent")
            , clicksDF("ip"), clicksDF("region"), citiesDF("city"), statesDF("state"), clicksDF("payingPrice"), clicksDF("biddingPrice")
            , logTypeDF("logType"), tagsDF("tag"))

        val clicksRDD = clicksFullDF.rdd
          .map(i => ((i(2).toString + i(1).toString + i(11).toString).hashCode, ClickFull(i(0).toString, i(1).toString, i(2).toString, getDeviceType(i(3).toString), i(4).toString, i(5).toString, i(6).toString, i(7).toString, i(8).toString, i(9).toString, i(10).toString, i(11).toString)))

        clicksRDD.toHBaseTable("click_full")
          .inColumnFamily("c")
          .toColumns("bidId"
            , "ts"
            , "ipyId"
            , "device"
            , "ip"
            , "region"
            , "city"
            , "state"
            , "payP"
            , "bidP"
            , "log"
            , "tag")
          .save()

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  implicit def clickFullWriter: FieldWriter[ClickFull] = new FieldWriterProxy[ClickFull, (String, String, String, String, String, String, String, String, String, String, String, String)] {
    override def convert(data: ClickFull) = (data.bidId
      , data.timestamp
      , data.ipinyouId
      , data.device
      , data.ip
      , data.region
      , data.city
      , data.state
      , data.payingPrice
      , data.biddingPrice
      , data.logType
      , data.tag)
  }

  implicit def getDeviceType(userAgentString: String): String = {
    val userAgent: UserAgent = new UserAgent(userAgentString)
    if (userAgent.getOperatingSystem != null && userAgent.getOperatingSystem.getDeviceType != null && userAgent.getOperatingSystem.getDeviceType.getName != null)
      userAgent.getOperatingSystem.getDeviceType.getName
    else
      "unknown"
  }
}

object Cities {

  @volatile private var instance: Broadcast[HBaseReaderBuilder[(String, String, String)]] = null

  def getInstance(sc: SparkContext): Broadcast[HBaseReaderBuilder[(String, String, String)]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val cities: HBaseReaderBuilder[(String, String, String)] = sc.hbaseTable[(String, String, String)]("city_us")
            .select("city", "stateId")
            .inColumnFamily("c")

          instance = sc.broadcast(cities)
        }
      }
    }
    instance
  }
}

object States {

  @volatile private var instance: Broadcast[HBaseReaderBuilder[(String, String)]] = null

  def getInstance(sc: SparkContext): Broadcast[HBaseReaderBuilder[(String, String)]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val cities: HBaseReaderBuilder[(String, String)] = sc.hbaseTable[(String, String)]("state_us")
            .select("state")
            .inColumnFamily("s")

          instance = sc.broadcast(cities)
        }
      }
    }
    instance
  }
}

object LogType {

  @volatile private var instance: Broadcast[HBaseReaderBuilder[(String, String)]] = null

  def getInstance(sc: SparkContext): Broadcast[HBaseReaderBuilder[(String, String)]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val cities: HBaseReaderBuilder[(String, String)] = sc.hbaseTable[(String, String)]("log_type")
            .select("type")
            .inColumnFamily("t")

          instance = sc.broadcast(cities)
        }
      }
    }
    instance
  }
}

object Tags {

  @volatile private var instance: Broadcast[HBaseReaderBuilder[(String, String)]] = null

  def getInstance(sc: SparkContext): Broadcast[HBaseReaderBuilder[(String, String)]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val cities: HBaseReaderBuilder[(String, String)] = sc.hbaseTable[(String, String)]("tag_us")
            .select("keywords")
            .inColumnFamily("kw")

          instance = sc.broadcast(cities)
        }
      }
    }
    instance
  }
}