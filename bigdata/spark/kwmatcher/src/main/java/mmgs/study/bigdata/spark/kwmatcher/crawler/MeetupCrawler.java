package mmgs.study.bigdata.spark.kwmatcher.crawler;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 *
 */
public class MeetupCrawler implements SNCrawler, Serializable {
    private static final String BASE_REQUEST = "https://api.meetup.com/2/";
    private static final String EVENTS_REQUEST = "open_events";
    private static final String EVENTS_COLUMNS = "id,description";
    private static final String PLACES_REQUEST = "open_venues";
    private static final String RESPONSE_FORMAT = "?text_format=plain";
    private static final int SUCCESS = 200;

    private static final String DATE_FORMAT = "yyyyMMdd";

    private boolean dataExtracted(HttpResponse<JsonNode> response) {
        return response.getStatus() == SUCCESS;
    }

    @Override
    public List<SNItem> extractEvents(TaggedClick taggedClick, String connectionKey) throws Exception {
        // TODO: move hardcode to constants
        // date range to milliseconds
        LocalDate date = LocalDate.parse(taggedClick.getDay(), DateTimeFormatter.ofPattern(DATE_FORMAT));
        ZonedDateTime zDate = date.atStartOfDay().atZone(ZoneId.of("Etc/UTC"));
        String queryDate = "" + zDate.minusWeeks(1).toInstant().toEpochMilli() + ',' + zDate.plusWeeks(1).toInstant().toEpochMilli();

        HttpResponse<JsonNode> jsonResponse = Unirest.get(BASE_REQUEST + EVENTS_REQUEST + RESPONSE_FORMAT)
                .queryString("key", connectionKey)
                .queryString("sign", "true")
                .queryString("only", EVENTS_COLUMNS)
                .queryString("status", "upcoming")
                .queryString("text", taggedClick.getTags())
                .queryString("time", queryDate)
                .queryString("lat", Double.toString(taggedClick.getLatitude()))
                .queryString("lon", Double.toString(taggedClick.getLongitude()))
                .asJson();

        if (dataExtracted(jsonResponse)) {
            return SNItem.fromJSON(jsonResponse.getBody().getObject().getJSONArray("results"));
        } else {
            // TODO: handle exceptions properly
            System.out.println(jsonResponse.getStatus());
            throw new Exception("Something went wrong");
        }
    }
}