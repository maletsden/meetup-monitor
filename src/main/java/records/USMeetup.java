package records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import scala.Serializable;
import java.util.HashMap;

import static helpers.UsStateCodeToName.usStateCodeToName;


public final class USMeetup implements Serializable {
    public final Event event;
    public final String group_city;
    public final String group_country;
    public final String group_id;
    public final String group_name;
    public final String group_state;

    public USMeetup(
            HashMap<String, Object> meetup
    ) {
        HashMap<String, Object> eventMap = (HashMap<String, Object>) meetup.get("event");
        HashMap<String, Object> groupMap = (HashMap<String, Object>) meetup.get("group");

        event = new Event(
                eventMap.get("event_name").toString(),
                eventMap.get("event_id").toString(),
                Long.parseLong(eventMap.get("time").toString())
        );
        group_city = groupMap.get("group_city").toString();
        group_country = groupMap.get("group_country").toString();
        group_id = groupMap.get("group_id").toString();
        group_name = groupMap.get("group_name").toString();
        group_state = isUS() ? usStateCodeToName(groupMap.get("group_state").toString()) : "";
    }

    @JsonIgnore
    public boolean isUS() {
        return group_country.equalsIgnoreCase("us");
    }
}
