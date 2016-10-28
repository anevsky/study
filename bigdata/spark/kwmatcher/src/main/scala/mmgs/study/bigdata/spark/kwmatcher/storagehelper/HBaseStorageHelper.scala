package mmgs.study.bigdata.spark.kwmatcher.storagehelper

import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick
import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.sql.SQLContext

object HBaseStorageHelper {

  def readTaggedClicks(sqlContext: SQLContext) = {
    // TODO: Configuratiion is to be moved to global settings
    val configuration = new Configuration()
    val df = sqlContext.phoenixTableAsDataFrame(
      "TAG_CLICK"
      , Array("ID", "DT", "KW", "LAT", "LON")
      // TODO: predicate is to be specified (by date, parametrized)
      //, predicate = Some("\"LAT\" = 40.6643")
      , conf = configuration
    )

    // TODO: remove hardcode for zookeeper settings
    /*    val df = sqlContext.read.format("org.apache.phoenix.spark")
          .options(Map("table" -> "TAG_CLICK"
            , "zkUrl" -> "127.0.0.1:2181"
            , "predicate" -> "LAT = 40.6643"
          )).load()*/
    df.show()
    df.map(i => new TaggedClick(i(0).toString, i(1).toString, i(3).toString.toDouble, i(4).toString.toDouble, i(2).toString)).toJavaRDD()
  }
}
