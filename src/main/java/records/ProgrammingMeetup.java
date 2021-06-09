package records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import scala.Serializable;

import java.util.*;
import java.util.stream.Collectors;

import static helpers.UsStateCodeToName.usStateCodeToName;

public class ProgrammingMeetup implements Serializable {
    public final Event event;
    public final List<String> group_topics;
    public final String group_city;
    public final String group_country;
    public final String group_id;
    public final String group_name;
    public final String group_state;

    private static final Set<String> PROGRAMMING_TOPICS = new HashSet<>(Arrays.asList(
            "Computer programming",
            "Big Data",
            "Machine Learning",
            "Python",
            "Java",
            "Web Development"
    ));

    public ProgrammingMeetup(
            HashMap<String, Object> meetup
    ) {
        HashMap<String, Object> eventMap = (HashMap<String, Object>) meetup.get("event");
        HashMap<String, Object> groupMap = (HashMap<String, Object>) meetup.get("group");
        List<HashMap<String, String>> group_topics_ = (List<HashMap<String, String>>) groupMap.get("group_topics");

        event = new Event(
                eventMap.get("event_name").toString(),
                eventMap.get("event_id").toString(),
                Long.parseLong(eventMap.get("time").toString())
        );
        group_topics = group_topics_
                .stream()
                .map(group_topic -> group_topic.get("topic_name"))
                .collect(Collectors.toList());
        group_city = groupMap.get("group_city").toString();
        group_country = groupMap.get("group_country").toString();
        group_id = groupMap.get("group_id").toString();
        group_name = groupMap.get("group_name").toString();
        group_state = group_country.equalsIgnoreCase("us")
                ? usStateCodeToName(groupMap.get("group_state").toString())
                : "";
    }

    @JsonIgnore
    public boolean isProgramming() {
        for (String topic: group_topics) {
            if (PROGRAMMING_TOPICS.contains(topic)) {
                return true;
            }
        }

        return false;
    }
}
