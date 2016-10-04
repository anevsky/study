package mmgs.study.bigdata.spark;

import com.restfb.*;
import com.restfb.types.Event;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FBExtractor implements Serializable {
    private static FacebookClient fbClient;
    private static FBExtractor fbExtractor;

    private static final String FIELDS = "fields";
    private static final String SEARCH_FIELDS = "id,name,description,attending_count,place";
    private static final String SEARCH = "search";
    private static final String Q = "q";
    private static final String SEARCH_TYPE = "type";
    private static final String SEARCH_EVENT = "event";

    private FBExtractor(){}

    public static FBExtractor getInstance(String fbUserToken) {
        if (fbClient == null) {
            fbClient = new DefaultFacebookClient(fbUserToken, Version.VERSION_2_7);
            fbExtractor = new FBExtractor();
        }
        return fbExtractor;
    }

    public PlaceVisitors extractEventVisitors(String date, String city, String keyword) {
        PlaceVisitors visitors = new PlaceVisitors();
        return visitors;
    }

    public static void main(String[] args) {
        FBExtractor extractor = FBExtractor.getInstance("EAACEdEose0cBABYx64s4SSIEDPOmmpfQg0fmK5DqaR894HvvrF5pGFCPZAYtVA4ax25axRooXtKhxFO1mPZAzWtCc8DKFbn4RATvruzyqH77EtrIuqY8O4YWeNMBnQ6U6wcOn2AvmCvoM7IpnficecpTJLnZCzDF1UUjG1wMNswJgVDHUGj");
        String keyword = "auto";
        Connection<Event> eventsSearch = extractor.fbClient.fetchConnection(SEARCH, Event.class
                , Parameter.with(Q, keyword)
                , Parameter.with(SEARCH_TYPE, SEARCH_EVENT)
                //
                //, Parameter.with("since")
                //, Parameter.with("until")
                , Parameter.with(FIELDS, SEARCH_FIELDS)
        );

        List<PlaceVisitors> visitorsList = new ArrayList<>();
        for (List<Event> events : eventsSearch) {
            for (Event event : events) {
                if (keyword.equals(event.getPlace().getLocation().getCity())) {
                    PlaceVisitors visitors = new PlaceVisitors();
                }
            }
        }
        System.out.println(eventsSearch.getData().toString());
    }

}
