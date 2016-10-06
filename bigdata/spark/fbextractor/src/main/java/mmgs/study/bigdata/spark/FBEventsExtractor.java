package mmgs.study.bigdata.spark;

import com.restfb.*;
import com.restfb.types.Event;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class FBEventsExtractor implements Serializable {

    private static FacebookClient fbClient;
    private static FBEventsExtractor fbEventsExtractor;

    private static final String FIELDS = "fields";
    private static final String SEARCH = "search";
    private static final String Q = "q";
    private static final String SEARCH_TYPE = "type";

    private static final String SEARCH_FIELDS = "id,name,description,attending_count,place,start_time";
    private static final String SEARCH_EVENT = "event";

    private FBEventsExtractor() {
    }

    public static FBEventsExtractor getInstance(String fbUserToken) {
        if (fbClient == null) {
            fbClient = new DefaultFacebookClient(fbUserToken, Version.VERSION_2_7);
            fbEventsExtractor = new FBEventsExtractor();
        }
        return fbEventsExtractor;
    }

    public void extract(String pattern) throws IOException {
        Connection<Event> eventsSearch = fbClient.fetchConnection(SEARCH, Event.class
                , Parameter.with(Q, pattern)
                , Parameter.with(SEARCH_TYPE, SEARCH_EVENT)
                , Parameter.with(FIELDS, SEARCH_FIELDS)
        );

        for (List<Event> extractedEvents : eventsSearch) {
            //fbEventsAsRDD(extractedEvents);
        }
    }

}
