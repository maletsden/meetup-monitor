import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

public class MeetupMonitorResultsSaver {

    public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {
        if (args.length < 3) {
            System.out.println(
                    "Usage: MeetupMonitor <brokers> <inputTopics> <bucketName> \n" +
                            "<brokers> is a list of one or more Kafka brokers to consume from\n" +
                            "<inputTopics> is a comma-separated list of input Kafka topics\n" +
                            "<bucketName> is a name of GCS bucket, where results will be saved\n"
            );
            System.exit(1);
        }

        String kafkaBrokers = args[0];
        String[] inputKafkaTopics = args[1].split(",");
        String bucketName = args[2] + (args[2].endsWith("/") ? "" : "/");

        SparkSession sparkSession = SparkSession.builder()
                .appName("MeetupMonitorResultsSaver")
                .getOrCreate();

        List<StreamingQuery> queries = new ArrayList<>();

        for (String kafkaTopic : inputKafkaTopics) {
            System.out.println("kafkaTopic: " + kafkaTopic);
            StreamingQuery query = sparkSession

                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaBrokers)
                    .option("subscribe", kafkaTopic)
                    .option("failOnDataLoss", "false")
                    .load()
                    .selectExpr("CAST(value as STRING)")
                    .select(col("value"))
                    .coalesce(1)

                    .writeStream()
                    .format("json")
                    .trigger(Trigger.ProcessingTime(10, TimeUnit.MINUTES))
                    .outputMode("append")
                    .option("path", String.format("%s/results/%s", bucketName, kafkaTopic))
                    .option("checkpointLocation", String.format("checks-%s", kafkaTopic))
                    .start();

            queries.add(query);
        }

        for (StreamingQuery query: queries) {
            query.awaitTermination();
        }

    }
}
