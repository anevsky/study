package mmgs.study.bigdata.spark.fbudf;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class FBEventData implements Serializable {
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
        return keywords;
    }

    public void setKeywords(Map<String, Integer> keywords) {
        this.keywords = keywords;
    }

    public void setKeywords(String text) {
        this.keywords = new HashMap<>();
        KeyWordsExtractor.keywordsCount(text).forEach((k, v) -> this.keywords.merge(k, v, Integer::sum));
    }
}
