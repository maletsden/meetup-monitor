package analyzers;

import helpers.ObjectToJSONMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import records.USMeetup;

import java.util.HashMap;

public class USMeetupFilter {
    private final KafkaProducer<String, String> producer;
    private final String topicName;

    public USMeetupFilter(
            KafkaProducer<String, String> producer_, String topicName_
    ) {
        producer = producer_;
        topicName = topicName_;
    }

    public void apply(HashMap<String, Object> meetupMap) {
        USMeetup meetup;
        try {
            meetup = new USMeetup(meetupMap);
        } catch (NullPointerException e) {
            return;
        }

        if (!meetup.isUS()) return;

        ProducerRecord<String, String> record = new ProducerRecord<>(
                topicName, "", ObjectToJSONMapper.objToJSON(meetup)
        );

        producer.send(record);
    }
}
