package mmgs.study.bigdata.spark;

import java.io.Serializable;
import java.util.Map;

public class PlaceVisitors implements Serializable {
    public String date;
    public String city;
    public Integer visitorsAmt;
    public Map<String, Integer> keywords;

    public String getDate() {
        return date;
    }

    public String getCity() {
        return city;
    }

    public Integer getVisitorsAmt() {
        return visitorsAmt;
    }

    public Map<String, Integer> getKeywords() {
        return keywords;
    }
}
