package mmgs.study.bigdata.spark

case class ClickInfo(bidId: String,
                 timestamp: String,
                 ipinyouId: String,
                 userAgent: String,
                 ip: String,
                 region: String,
                 city: String,
                 payingPrice: String,
                 biddingPrice: String,
                 streamId: String,
                 userTags: String
                )

case class ClickAdInfo(
                        adExchange: String,
                        domain: String,
                        url: String,
                        anonimousUrlId: String,
                        adSlotId: String,
                        adSlotWidth: String,
                        adSlotHeight: String,
                        adSlotVisibility: String,
                        adSlotFormat: String,
                        creativeId: String,
                        advertiserId: String
                      )

class Click {

}
