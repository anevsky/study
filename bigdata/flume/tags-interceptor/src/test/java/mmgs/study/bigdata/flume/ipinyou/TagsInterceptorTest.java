package mmgs.study.bigdata.flume.ipinyou;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TagsInterceptorTest {

    @Test
    public void tagsMapLoaded() {
        String tags = TagsInterceptor.getTags("282163091140");
        assertThat("Expected tag value is present in cached tags map", tags, is("motorcycle,lighting"));
    }

    @Test
    public void interceptSingleTest() {
        String initialEventBody = "2a72c8727a42de2ceaaaf8b17d7654d5\t20130606001907462\tVhkSLxSELTuOkGn\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729)\t27.36.3.*\t216\t222\t3\ttMxYQ19aM98\ta0ab67bbe87f2e024338b093856580\tnull\tLV_1001_LDVi_LD_ADX_2\t300\t250\t0\t0\t100\t00fccc64a1ee2809348509b7ac2a97a5\t241\t3427\t282163094182\t0";
        String tagsToAdd = "tracker,gps,vehicle,gsm,mini,car,gprs,time,real,locater";
        String finalEventBody = initialEventBody + "\t" + tagsToAdd;

        TagsInterceptor tagsInterceptor = new TagsInterceptor();
        Event event = new SimpleEvent();
        event.setBody(initialEventBody.getBytes());

        Event intercept = tagsInterceptor.intercept(event);
        String newEventBody = new String(intercept.getBody());
        assertThat("Body is enriched correctly", newEventBody, is(equalTo(finalEventBody)));
    }

    @Test
    public void interceptNotExistingTag() {
        String initialEventBody = "2a72c8727a42de2ceaaaf8b17d7654d5\t20130606001907462\tVhkSLxSELTuOkGn\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729)\t27.36.3.*\t216\t222\t3\ttMxYQ19aM98\ta0ab67bbe87f2e024338b093856580\tnull\tLV_1001_LDVi_LD_ADX_2\t300\t250\t0\t0\t100\t00fccc64a1ee2809348509b7ac2a97a5\t241\t3427\t12345\t0";
        String tagsToAdd = "";
        String finalEventBody = initialEventBody + "\t" + tagsToAdd;

        TagsInterceptor tagsInterceptor = new TagsInterceptor();
        Event event = new SimpleEvent();
        event.setBody(initialEventBody.getBytes());

        Event intercept = tagsInterceptor.intercept(event);
        String newEventBody = new String(intercept.getBody());
        assertThat("Body is enriched correctly", newEventBody, is(equalTo(finalEventBody)));
    }
}