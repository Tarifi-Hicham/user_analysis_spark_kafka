from pykafka import KafkaClient
import json

topic_name = "randomuser_data"
kafka_hosts = "localhost:9092"  # Change this to your Kafka server address

# Create a Kafka client
client = KafkaClient(hosts=kafka_hosts)
topic = client.topics[topic_name]
consumer = topic.get_simple_consumer()

# Consume messages from the Kafka topic
for message in consumer:
    if message is not None:
        data = json.loads(message.value.decode('utf-8'))
        # Process the data as needed
        print(data)  # Example: Print the data to the console

# Close the consumer connection (optional)
consumer.stop()