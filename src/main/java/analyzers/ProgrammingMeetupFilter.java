package analyzers;

import helpers.ObjectToJSONMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import records.ProgrammingMeetup;

import java.util.HashMap;

public class ProgrammingMeetupFilter {
    private final KafkaProducer<String, String> producer;
    private final String topicName;

    public ProgrammingMeetupFilter(
            KafkaProducer<String, String> producer_, String topicName_
    ) {
        producer = producer_;
        topicName = topicName_;
    }

    public void apply(HashMap<String, Object> meetupMap) {
        ProgrammingMeetup meetup = new ProgrammingMeetup(meetupMap);

        if (!meetup.isProgramming()) return;

        ProducerRecord<String, String> record = new ProducerRecord<>(
                topicName, "", ObjectToJSONMapper.objToJSON(meetup)
        );

        producer.send(record);
    }

}
