package mmgs.study.bigdata.spark;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FBEventDataTest {
    @Test
    public void toJson() throws Exception {
        FBEventData event = new FBEventData();
        event.setEventId("12345");
        event.setDate("20160906");
        event.setCity("New York");
        event.setVisitorsAmt(10);
        Map<String, Integer> keywords = new HashMap<>();
        keywords.put("something", 1);
        keywords.put("everything", 2);
        event.setKeywords(keywords);

        String jsonExpected = "{\"eventId\":\"12345\",\"date\":\"20160906\",\"city\":\"New York\",\"visitorsAmt\":10,\"keywords\":{\"everything\":2,\"something\":1}}";
        assertThat("Json for populated object is correct", jsonExpected, equalTo(event.toJson()));
    }

}