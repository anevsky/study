package mmgs.study.bigdata.spark;

import com.restfb.*;
import com.restfb.types.Event;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.Thread.sleep;

public class FBEventsCrawler {

    private static final String FIELDS = "fields";
    private static final String SEARCH = "search";
    private static final String Q = "q";
    private static final String SEARCH_TYPE = "type";

    private static final String SEARCH_FIELDS = "id,name,description,attending_count,place,start_time";
    private static final String SEARCH_EVENT = "event";

    public static void main(String[] args) throws IOException, ParseException {
        String datasetDir = args[0];
        String dateSince = args[1];
        String dateUntil = args[2];
        String fbUserToken = args[3];

        System.setProperty("hadoop.home.dir", "d:/Software/hadoop-2.7.3/");
        System.setProperty("spark.home.dir", "d:/Software/spark-2.0.0-bin-hadoop2.7/");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Ugly FB events Crawler");
        sparkConf.set("spark.sql.warehouse.dir", "file:///d:/tmp/spark-warehouse");
        sparkConf.setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // calculate filters based on provided datasets
        // cities
        JavaRDD<String> citiesRDD = sparkContext.textFile(datasetDir + "/dict/city.us.txt");
        List<String> citiesList = citiesRDD.map(x -> x.split("\t")).map(x -> x[1]).collect();
        // keywords
        JavaRDD<String> keywordsRDD = sparkContext.textFile(datasetDir + "/dict/user.profile.tags.us.txt");
        String keywordsHeader = keywordsRDD.first();
        List<String> keywordsList = keywordsRDD.filter(x -> !x.equals(keywordsHeader)).map(x -> x.split("\t")).map(x -> x[1]).flatMap(x -> Arrays.asList(x.split(",")).iterator()).distinct().collect();

        // dates
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        Date since = subtractDays(formatter.parse(dateSince), 1);
        Date until = addDays(formatter.parse(dateUntil), 1);

        // query FB for events with specified keywords
        FacebookClient fbClient = new DefaultFacebookClient(fbUserToken, Version.VERSION_2_7);

        // final results will be
        JavaRDD<FBEventData> fbEventsRDD = sparkContext.emptyRDD();
        for (String keyword : keywordsList) {
            Connection<Event> eventsSearch = fbClient.fetchConnection(
                    SEARCH
                    , Event.class
                    , Parameter.with(Q, keyword)
                    , Parameter.with(SEARCH_TYPE, SEARCH_EVENT)
                    , Parameter.with(FIELDS, SEARCH_FIELDS)

            );

            for (List<Event> extractedEvents : eventsSearch) {
                List<String> fbEvents = new ArrayList<>();
                for (Event extractedEvent : extractedEvents) {
                    if ((extractedEvent.getPlace() != null
                            && extractedEvent.getPlace().getLocation() != null
                            && extractedEvent.getPlace().getLocation().getCity() != null)) {
                        if (extractedEvent.getPlace().getLocation().getCountry() != null
                                && extractedEvent.getPlace().getLocation().getCountry().equals("United States")) {
                            if (extractedEvent.getStartTime() != null) {
                                if (extractedEvent.getStartTime().after(since)
                                        & extractedEvent.getStartTime().before(until)) {
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
                                    if (extractedEvent.getDescription() != null) {
                                        fbEvent.setKeywords(extractedEvent.getDescription());
                                    }
                                    fbEvent.setSourceKeyword(keyword);
                                    try {
                                        fbEvents.add(fbEvent.toJson());
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }
                    }
                }
                if (!fbEvents.isEmpty()) {
                    JavaRDD tmp = sparkContext.parallelize(fbEvents);
                    fbEventsRDD = sparkContext.union(fbEventsRDD, tmp);
                }
            }
            try {
                sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        fbEventsRDD.saveAsTextFile(datasetDir + "/out/fb-events.txt");
    }

    public static Date addDays(Date date, int days) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, days);
        return cal.getTime();
    }

    public static Date subtractDays(Date date, int days) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, -days);
        return cal.getTime();
    }
}
