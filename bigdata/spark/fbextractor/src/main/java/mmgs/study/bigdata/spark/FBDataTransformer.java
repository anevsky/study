package mmgs.study.bigdata.spark;

import com.restfb.types.Event;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FBDataTransformer {
    private static JavaSparkContext sparkContext;
    private static FBDataTransformer fbDataTransformer;

    public static FBDataTransformer getInstance(SparkConf sparkConf) {
        if (fbDataTransformer == null) {
            fbDataTransformer = new FBDataTransformer();
            sparkContext = new JavaSparkContext(sparkConf);
        }
        return fbDataTransformer;
    }

    public JavaRDD fbEventsAsRDD(List<Event> extractedEvents) throws IOException {
        List<String> fbEvents = new ArrayList<>();
        for (Event extractedEvent : extractedEvents) {
            FBEventData fbEvent = new FBEventData();
            fbEvent.setEventId(extractedEvent.getId());
            if (extractedEvent.getStartTime() != null) {
                fbEvent.setDate(extractedEvent.getStartTime());
            }
            if (extractedEvent.getPlace() != null
                    && extractedEvent.getPlace().getLocation() != null
                    && extractedEvent.getPlace().getLocation().getCity() != null) {
                fbEvent.setCity(extractedEvent.getPlace().getLocation().getCity());
            }
            fbEvent.setVisitorsAmt(extractedEvent.getAttendingCount());
            fbEvent.setKeywords(extractedEvent.getDescription());
            fbEvents.add(fbEvent.toJson());
        }
        return sparkContext.parallelize(fbEvents);
    }

}
