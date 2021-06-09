import analyzers.ProgrammingMeetupFilter;
import analyzers.USCitiesEveryMinuteReducer;
import analyzers.USMeetupFilter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import records.USCitiesEveryMinute;
import scala.Tuple2;

import java.util.*;


public class MeetupMonitor {

    public static void main(String[] args) throws InterruptedException {

        if (args.length < 3) {
            System.out.println(
                    "Usage: MeetupMonitor <brokers> <inputTopic> <outputTopics> \n" +
                    "<brokers> is a list of one or more Kafka topics to consume from\n" +
                    "<inputTopic> is the name of input Kafka topic for reading all meetups\n" +
                    "<outputTopics> is a comma-separated list of output Kafka topics\n"
            );
            System.exit(1);
        }

        String kafkaBrokers = args[0];
        String inputKafkaTopic = args[1];
        String[] outputTopics = args[2].split(",");
        String usMeetupsKafkaTopic = outputTopics[0];
        String usCitiesEveryMinuteKafkaTopic = outputTopics[1];
        String programmingMeetupsKafkaTopic = outputTopics[2];

        SparkConf conf = new SparkConf().setAppName("MeetupMonitor");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaBrokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "group_id_1");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Map<String, Object> kafkaOutputParams = new HashMap<>();
        kafkaOutputParams.put("bootstrap.servers", kafkaBrokers);
        kafkaOutputParams.put("key.serializer", StringSerializer.class);
        kafkaOutputParams.put("value.serializer", StringSerializer.class);

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(Collections.singleton(inputKafkaTopic), kafkaParams)
                );


        JavaPairDStream<Long, String> windowedStream =
                stream
                        .mapToPair(record -> new Tuple2<>(record.timestamp(), record.value()))
                        .window(Durations.seconds(20), Durations.seconds(20));

        windowedStream.foreachRDD(rdd -> {
            if (rdd.isEmpty()) return;

            // parallel analyzers for each partition
            JavaRDD<USCitiesEveryMinute> reducedPartitionWindowStats = rdd.mapPartitions(
                    partition -> {
                        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaOutputParams);
                        ObjectMapper objectMapper = new ObjectMapper();

                        USMeetupFilter usMeetupFilter =
                                new USMeetupFilter(producer, usMeetupsKafkaTopic);
                        ProgrammingMeetupFilter programmingMeetupFilter =
                                new ProgrammingMeetupFilter(producer, programmingMeetupsKafkaTopic);

                        List<USCitiesEveryMinute> usCitiesEveryMinute = Collections.singletonList(
                                new USCitiesEveryMinute()
                        );

                        partition.forEachRemaining(
                                message -> {
                                    HashMap<String, Object> messageMap;
                                    Long messageTimestamp = message._1;
                                    String messageValue = message._2;

                                    try {
                                        messageMap = objectMapper.readValue(messageValue, HashMap.class);
                                    } catch (JsonProcessingException | NullPointerException e) {
                                        e.printStackTrace();

                                        return;
                                    }

                                    if (messageMap == null) return;

                                    usMeetupFilter.apply(messageMap);
                                    programmingMeetupFilter.apply(messageMap);
                                    usCitiesEveryMinute.get(0).merge(messageMap, messageTimestamp);
                                }
                        );

                        producer.close();

                        return usCitiesEveryMinute.iterator();
                    }
            );

            // write window statistic to Kafka
            USCitiesEveryMinute mergeReducer = reducedPartitionWindowStats.reduce(USCitiesEveryMinute::merge);
            KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaOutputParams);
            USCitiesEveryMinuteReducer usCitiesEveryMinuteReducer =
                                new USCitiesEveryMinuteReducer(producer, usCitiesEveryMinuteKafkaTopic, mergeReducer);

            usCitiesEveryMinuteReducer.send();

            producer.close();
        });

        streamingContext.start();              // Start the computation
        streamingContext.awaitTermination();   // Wait for the computation to terminate
    }
}
