package analyzers;

import helpers.ObjectToJSONMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import records.USCitiesEveryMinute;

import java.util.HashMap;

public class USCitiesEveryMinuteReducer {
    private final KafkaProducer<String, String> producer;
    private final String topicName;
    private final USCitiesEveryMinute result;

    public USCitiesEveryMinuteReducer(
            KafkaProducer<String, String> producer_, String topicName_, USCitiesEveryMinute result_
    ) {
        producer = producer_;
        topicName = topicName_;
        result = result_;
    }

    public void reduce(HashMap<String, Object> meetupMap, Long meetupTimestamp) {
        result.merge(meetupMap, meetupTimestamp);
    }

    public void send() {
        if (result.cities.isEmpty()) return;

        ProducerRecord<String, String> record = new ProducerRecord<>(
                topicName, "", ObjectToJSONMapper.objToJSON(result)
        );

        producer.send(record);
    }

    public USCitiesEveryMinuteReducer merge(
            USCitiesEveryMinuteReducer reducer
    ) {
        result.merge(reducer.result);

        return this;
    }
}
