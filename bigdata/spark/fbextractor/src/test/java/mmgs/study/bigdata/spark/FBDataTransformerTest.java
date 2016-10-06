package mmgs.study.bigdata.spark;

import com.restfb.types.Event;
import com.restfb.types.Location;
import com.restfb.types.Place;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class FBDataTransformerTest {
    private SparkConf sparkConf;
    private FBDataTransformer fbDataTransformer;

    @Before
    public void setUp() throws Exception {
        System.setProperty("hadoop.home.dir", "d:/Software/hadoop-2.7.3/");

        sparkConf = new SparkConf().setAppName("localTestApp");
        sparkConf.set("spark.sql.warehouse.dir", "file:///" + getClass().getResource("").toString());
        sparkConf.setMaster("local[*]");

        fbDataTransformer = FBDataTransformer.getInstance(sparkConf);
    }

    @Test
    public void fbEventsAsRddTest() throws IOException, ParseException {
        List<Event> fbEvents = new ArrayList<>();
        Event event = new Event();
        event.setId("12345");
        event.setName("Some event");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        event.setStartTime(formatter.parse("20160906"));
        Place place = new Place();
        Location location = new Location();
        location.setCity("Lardner");
        place.setLocation(location);
        event.setPlace(place);
        event.setDescription("Very long description of the event");
        event.setAttendingCount(11);
        fbEvents.add(event);
        JavaRDD rdd = fbDataTransformer.fbEventsAsRDD(fbEvents);

        String jsonExpected = "{\"eventId\":\"12345\",\"date\":\"20160906\",\"city\":\"Lardner\",\"visitorsAmt\":11,\"keywords\":{\"very\":1,\"description\":1,\"event\":1,\"long\":1}}";

        assertThat("RDD contains data", jsonExpected, equalTo(rdd.first().toString()));
        System.out.println("Stopplace");
    }

    @After
    public void tearDown() throws Exception {
    }

}