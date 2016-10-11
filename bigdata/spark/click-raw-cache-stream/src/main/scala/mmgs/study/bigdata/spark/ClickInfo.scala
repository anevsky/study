package mmgs.study.bigdata.spark

case class ClickInfo(
                      bidId: String,
                      timestamp: String,
                      ipinyouId: String,
                      userAgent: String,
                      ip: String,
                      region: String,
                      city: String,
                      payingPrice: Double,
                      biddingPrice: Double,
                      streamId: String,
                      adExchange: String,
                      domain: String,
                      url: String,
                      anonimousUrlId: String,
                      adSlotId: String,
                      adSlotWidth: Integer,
                      adSlotHeight: Integer,
                      adSlotVisibility: Short,
                      adSlotFormat: Short,
                      creativeId: String,
                      advertiserId: Integer,
                      userTags: Long
                    )
