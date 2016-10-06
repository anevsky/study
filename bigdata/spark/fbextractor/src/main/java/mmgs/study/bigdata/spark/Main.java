package mmgs.study.bigdata.spark;

import com.restfb.*;
import com.restfb.types.Event;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    private static final String FIELDS = "fields";
    private static final String SEARCH = "search";
    private static final String Q = "q";
    private static final String SEARCH_TYPE = "type";

    private static final String SEARCH_FIELDS = "id,name,description,attending_count,place,start_time";
    private static final String SEARCH_EVENT = "event";

    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "d:/Software/hadoop-2.7.3/");
        System.setProperty("spark.home.dir", "d:/Software/spark-2.0.0-bin-hadoop2.7/");
        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("localTestApp");
        sparkConf.set("spark.sql.warehouse.dir", "file:///d:/tmp/spark-warehouse");
        sparkConf.setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // get dictionary items:
        // cities
        JavaRDD<String> citiesRDD = sparkContext.textFile("file:///d:/Study/BigData/EPAM/MainCourse/09-Spark/dataset/dict/city.us.txt");
        List<String> citiesList = citiesRDD.map(x -> x.split("\t")).map(x -> x[1]).collect();
        // dates
        // keywords
        JavaRDD<String> keywordsRDD = sparkContext.textFile("file:///d:/Study/BigData/EPAM/MainCourse/09-Spark/dataset/dict//user.profile.tags.us.txt");
        String keywordsHeader = keywordsRDD.first();
        JavaRDD<String> map = keywordsRDD.filter(x -> !x.equals(keywordsHeader)).map(x -> x.split("\t")).map(x -> x[1]);


        JavaRDD<String> javaRDD = map.flatMap(x -> Arrays.asList(x.split(",")).iterator()).distinct();
        String keywordsString = javaRDD.collect().stream().map(Object::toString).collect(Collectors.joining(" | "));


        // query FB for events with specified keywords
        String fbUserToken = "";
        FacebookClient fbClient = new DefaultFacebookClient(fbUserToken, Version.VERSION_2_7);

        Connection<Event> eventsSearch = fbClient.fetchConnection(SEARCH, Event.class
                , Parameter.with(Q, keywordsString)
                , Parameter.with(SEARCH_TYPE, SEARCH_EVENT)
                , Parameter.with(FIELDS, SEARCH_FIELDS)
        );

        JavaRDD<FBEventData> fbEventsRDD = sparkContext.emptyRDD();

        for (List<Event> extractedEvents : eventsSearch) {
            List<String> fbEvents = new ArrayList<>();
            for (Event extractedEvent : extractedEvents) {
                if (extractedEvent.getPlace() != null
                        && extractedEvent.getPlace().getLocation() != null
                        && extractedEvent.getPlace().getLocation().getCity() != null){
                    if (extractedEvent.getPlace().getLocation().getCountry().equals("United States")) {
                        FBEventData fbEvent = new FBEventData();
                        fbEvent.setEventId(extractedEvent.getId());
                        if (extractedEvent.getStartTime() != null) {
                            fbEvent.setDate(extractedEvent.getStartTime());
                        }

                        if (citiesList.contains(extractedEvent.getPlace().getLocation().getCity())) {
                            fbEvent.setCity(extractedEvent.getPlace().getLocation().getCity());
                        } else {
                            fbEvent.setCity("unknown");
                        }
                        fbEvent.setVisitorsAmt(extractedEvent.getAttendingCount());
                        fbEvent.setKeywords(extractedEvent.getDescription());
                        fbEvents.add(fbEvent.toJson());
                    }
                }
            }
            if (!fbEvents.isEmpty()) {
                JavaRDD tmp = sparkContext.parallelize(fbEvents);
                fbEventsRDD = sparkContext.union(fbEventsRDD, tmp);
            }
        }

        System.out.println(fbEventsRDD.first());
    }
}
