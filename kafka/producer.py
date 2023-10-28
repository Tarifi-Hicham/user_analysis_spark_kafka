from pykafka import KafkaClient
import requests
import json
import time

topic_name = "randomuser_data"
kafka_hosts = "localhost:9092"  # Change this to your Kafka server address

# Create a Kafka client
client = KafkaClient(hosts=kafka_hosts)
topic = client.topics[topic_name]
producer = topic.get_producer()

while True:
    response = requests.get("https://randomuser.me/api/")
    data = json.dumps(response.json())
    producer.produce(data.encode('utf-8'))
    time.sleep(5)

# Close the producer connection (optional)
producer.stop()