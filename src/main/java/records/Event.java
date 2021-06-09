package records;

import scala.Serializable;

import java.util.Date;

public class Event implements Serializable {
    public final String event_name;
    public final String event_id;
    public final String time;

    public Event(
            String event_name_, String event_id_, Long time_
    ) {
        event_name = event_name_;
        event_id = event_id_;
        time = new Date(time_).toString();
    }
}
