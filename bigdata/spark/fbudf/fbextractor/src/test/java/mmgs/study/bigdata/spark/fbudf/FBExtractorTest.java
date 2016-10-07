package mmgs.study.bigdata.spark.fbudf;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class FBExtractorTest {
    private static FBExtractor extractor;

    // Both tests were used for debugging purposes and need to have user access token hard-coded within setUp method
    //  as we cannot publish the token, tests are ignored for final usage
    @BeforeClass
    @Ignore
    public static void setUp() {
        extractor = FBExtractor.getInstance("");
    }

    @Test
    @Ignore
    public void extractAttendees() throws Exception {
        String eventId = "1839492569612308";
        List<FBEventAttendee> attendees = extractor.extractAttendees(eventId);
        assertThat("attendees count matches", 699, equalTo(attendees.size()));
    }

    @Test
    @Ignore
    public void extractEvents() throws Exception {
        String keyword = "auto";
        String city = "Marion";
        String date = "20161119";
        List<FBEventData> fbEvents = extractor.extractEvents(date, city, keyword);
        assertThat("Events count matches", 1, equalTo(fbEvents.size()));
    }

}