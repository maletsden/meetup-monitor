# Meetup Monitor

Meetup Monitor is a simple monitoring tool for the meetup.com website. The website provides a stream data source with the newly created meetup events.

The main analyzing park of the project is written using the Java Spark-Streaming-Kafka library and for saving results to Google Cloud Storage Java Spark-SQL-Kafka library is used.

---

## Prerequisites

### producer.py

Producer.py is the simple python application that listens to the meetup stream and saves it directly to Kafka topic (meetups-input) and is running on the Kafka cluster.

Install all dependencies

```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

Set necessary environment variables

```bash
export MEETUP_STREAM_URL=<MEETUP_STREAM_URL>
export KAFKA_BOOTSTRAP_SERVERS=<KAFKA_BOOTSTRAP_SERVERS>
export KAFKA_OUTPUT_TOPIC_NAME=<KAFKA_OUTPUT_TOPIC_NAME>
```


---

### MeetupMonitor.java

MeetupMonitor is the main class that is used for analyzing meetup events.

Install all dependencies

```bash
mvn install
```

Study required arguments of the application

```textmate
Usage: MeetupMonitor <brokers> <inputTopic> <outputTopics>
<brokers> is a list of one or more Kafka brokers to consume from
<inputTopic> is the name of input Kafka topic for reading all meetups
<outputTopics> is a comma-separated list of output Kafka topics
```

---

### MeetupMonitorResultsSaver.java

MeetupMonitorResultsSaver is the main class that is used for saving results to Google Cloud Storage.

Install all dependencies

```bash
mvn install
```

Study required arguments of the application

```textmate
Usage: MeetupMonitor <brokers> <inputTopics> <bucketName>
<brokers> is a list of one or more Kafka brokers to consume from
<inputTopics> is a comma-separated list of input Kafka topics
<bucketName> is a name of GCS bucket, where results will be saved
```

---

## Start application

### producer.py

```bash
python producer.py
```

### MeetupMonitor.java

```bash
/spark-submit \
  --master <master> \
  --class MeetupMonitor \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2 \
  target/meetup-monitor-1.0-SNAPSHOT.jar \
  <brokers> <inputTopics> <bucketName>
```

### MeetupMonitorResultsSaver.java

```bash
/spark-submit \
  --master <master> \
  --class MeetupMonitorResultsSaver \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
  target/meetup-monitor-1.0-SNAPSHOT.jar \
  <brokers> <inputTopics> <bucketName>
```
