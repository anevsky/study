package mmgs.study.bigdata.flume.ipinyou;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.io.*;
import java.util.*;

public class TagsInterceptor implements Interceptor {

    private static final String tagsFileName = "user.profile.tags.us.txt.out";
    private static Map<String, String> tagsMap = new HashMap<>();
    private static String fileDelimiter = "\t";

    static {
        BufferedReader br = new BufferedReader(new InputStreamReader((TagsInterceptor.class.getClassLoader().getResourceAsStream(tagsFileName))));

        String line;
        String[] splittedLine;
        try {
            while ((line = br.readLine()) != null) {
                splittedLine = line.split(fileDelimiter);
                tagsMap.put(splittedLine[0], splittedLine[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static String getTags(String key) {
        String value = tagsMap.get(key);
        return value == null ? "" : value;
    }

    @Override
    public void initialize() {}

    private static String appendTags(String s, String tags) {
        return s.trim() + fileDelimiter + tags;

    }

    @Override
    public Event intercept(Event event) {
        String eventBody = new String(event.getBody());
        String[] splittedBody = eventBody.split(fileDelimiter);
        eventBody = appendTags(eventBody, getTags(splittedBody[20]));
        event.setBody(eventBody.getBytes());
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> interceptedEvents = new ArrayList<>(events.size());
        for (Event event : events) {
            // Intercept any event
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }
        return interceptedEvents;
    }

    @Override
    public void close() {}

    public static class Builder implements Interceptor.Builder
    {
        @Override
        public void configure(Context context) {
            // TODO Auto-generated method stub
        }

        @Override
        public Interceptor build() {
            return new TagsInterceptor();
        }
    }
}
