package records;

import scala.Serializable;
import java.util.*;

public class USCitiesEveryMinute implements Serializable {
    public Integer month;
    public Integer day_of_the_month;
    public Integer hour;
    public Integer minute;
    public final Set<String> cities;

    private Long timestamp;

    public USCitiesEveryMinute(
            HashMap<String, Object> meetup, Long timestamp_
    ) {
        HashMap<String, Object> groupMap = (HashMap<String, Object>) meetup.get("group");
        cities = new HashSet<>();
        cities.add(groupMap.get("group_city").toString());

        updateDate(timestamp_);
    }

    public USCitiesEveryMinute() {
        cities = new HashSet<>();

        updateDate(0L);
    }

    public USCitiesEveryMinute merge(
            HashMap<String, Object> meetup, Long timestamp_
    ) {
        HashMap<String, Object> groupMap = (HashMap<String, Object>) meetup.get("group");
        cities.add(groupMap.get("group_city").toString());

        if (timestamp_ > timestamp) {
            updateDate(timestamp_);
        }

        return this;
    }

    public USCitiesEveryMinute merge(
            USCitiesEveryMinute record
    ) {
        cities.addAll(record.cities);

        if (record.timestamp > timestamp) {
            month = record.month;
            day_of_the_month = record.day_of_the_month;
            hour = record.hour;
            minute = record.minute;

            timestamp = record.timestamp;
        }

        return this;
    }

    private void updateDate(Long timestamp_) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestamp_);

        month = cal.get(Calendar.MONTH);
        day_of_the_month = cal.get(Calendar.DAY_OF_MONTH);
        hour = cal.get(Calendar.HOUR_OF_DAY);
        minute = cal.get(Calendar.MINUTE);

        timestamp = timestamp_;
    }
}
