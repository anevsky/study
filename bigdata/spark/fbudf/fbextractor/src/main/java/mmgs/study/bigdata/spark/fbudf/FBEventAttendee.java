package mmgs.study.bigdata.spark.fbudf;

import java.io.Serializable;

public class FBEventAttendee implements Serializable {
    private String userId;
    private String eventId;
    private String userName;

    public FBEventAttendee(String userId, String userName, String eventId) {
        this.userId = userId;
        this.eventId = eventId;
        this.userName = userName;
    }

    public String getUserId() {
        return userId;
    }

    public String getEventId() {
        return eventId;
    }

    public String getUserName() {
        return userName;
    }

}
