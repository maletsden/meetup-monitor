import os
import requests
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'))

r = requests.get(os.environ.get('MEETUP_STREAM_URL'), stream=True)

for line in r.iter_lines():
    try:
        future = producer.send(os.environ.get('KAFKA_OUTPUT_TOPIC_NAME'), line)
        result = future.get(timeout=1)
    except Exception as e:
        print(e)
