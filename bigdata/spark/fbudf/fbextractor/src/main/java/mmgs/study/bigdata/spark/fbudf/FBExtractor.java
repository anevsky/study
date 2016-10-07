package mmgs.study.bigdata.spark.fbudf;

import com.restfb.*;
import com.restfb.types.Event;
import com.restfb.types.User;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class FBExtractor implements Serializable {
    private static final String SEARCH_TYPE = "search";
    private static final String SEARCH_PARAM_KEYWORDS = "q";
    private static final String SEARCH_ENTITY_PARAM = "type";
    private static final String SEARCH_FIELDS_PARAM = "fields";
    private static final String SEARCH_ENTITY_EVENT = "event";
    private static final String SEARCH_FIELDS_EVENT = "id,name,description,attending_count,place";
    private static final String SEARCH_PARAM_ATTENDING = "/attending";

    private static final DateFormat DATE_FORMATTER = new SimpleDateFormat("yyyyMMdd");

    private static FacebookClient fbClient;
    private static FBExtractor fbExtractor;

    private FBExtractor() {
    }

    public static FBExtractor getInstance(String fbUserToken) {
        if (fbClient == null) {
            fbClient = new DefaultFacebookClient(fbUserToken, Version.VERSION_2_7);
            fbExtractor = new FBExtractor();
        }
        return fbExtractor;
    }

    public List<FBEventData> extractEvents(String date, String city, String keyword) {
        Connection<Event> eventsSearch = fbClient.fetchConnection(SEARCH_TYPE, Event.class
                , Parameter.with(SEARCH_PARAM_KEYWORDS, keyword)
                , Parameter.with(SEARCH_ENTITY_PARAM, SEARCH_ENTITY_EVENT)
                , Parameter.with(SEARCH_FIELDS_PARAM, SEARCH_FIELDS_EVENT)
        );

        List<FBEventData> events = new ArrayList<>();
        for (List<Event> extractedEvents : eventsSearch) {
            for (Event extractedEvent : extractedEvents) {
                // we are looking for events only from specific city
                // city must be specified
                if (extractedEvent.getPlace() != null
                        && extractedEvent.getPlace().getLocation() != null
                        && extractedEvent.getPlace().getLocation().getCity() != null
                        && city.equals(extractedEvent.getPlace().getLocation().getCity())) {
                    // we are looking for events only within specific dates range
                    if (extractedEvent.getStartTime() != null && date.equals(DATE_FORMATTER.format(extractedEvent.getStartTime()))) {
                        FBEventData event = new FBEventData();
                        event.setEventId(extractedEvent.getId());
                        event.setSourceKeyword(keyword);
                        event.setDate(date);
                        if (extractedEvent.getDescription() != null) {
                            event.setKeywords(extractedEvent.getDescription());
                        }
                        event.setCity(city);
                        event.setVisitorsAmt(extractedEvent.getAttendingCount());
                        events.add(event);
                    }
                }
            }
        }
        return events;
    }

    public List<FBEventAttendee> extractAttendees(String eventId) {
        Connection<User> attendeesSearch = fbClient.fetchConnection(eventId + SEARCH_PARAM_ATTENDING, User.class);
        List<FBEventAttendee> attendees = new ArrayList<>();
        for (List<User> extractedAttendees : attendeesSearch) {
            for (User extractedAttendee : extractedAttendees) {
                attendees.add(new FBEventAttendee(extractedAttendee.getId(), extractedAttendee.getName(), eventId));
            }
        }
        return attendees;
    }
}
