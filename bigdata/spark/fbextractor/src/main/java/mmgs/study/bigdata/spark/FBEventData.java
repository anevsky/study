package mmgs.study.bigdata.spark;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class FBEventData implements Serializable {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");

    private String eventId;
    private String sourceKeyword;
    private String date;
    private String city;
    private Integer visitorsAmt;
    private Map<String, Integer> keywords;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getSourceKeyword() {
        return sourceKeyword;
    }

    public void setSourceKeyword(String sourceKeyword) {
        this.sourceKeyword = sourceKeyword;
    }

    public String getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = dateFormatter.format(date);
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Integer getVisitorsAmt() {
        return visitorsAmt;
    }

    public void setVisitorsAmt(Integer visitorsAmt) {
        this.visitorsAmt = visitorsAmt;
    }

    public Map<String, Integer> getKeywords() {
        return keywords != null ? Collections.unmodifiableMap(keywords) : null;
    }

    public void setKeywords(String text) {
        this.keywords = new HashMap<>();
        KeyWordsExtractor.keywordsCount(text).forEach((k, v) -> this.keywords.merge(k, v, Integer::sum));
    }

    public void setKeywords(Map<String, Integer> keywords) {
        this.keywords = new HashMap<>(keywords);
    }

    public String toJson() throws IOException {
        return objectMapper.writeValueAsString(this);
    }
}
