from pykafka import KafkaClient
import json

# Kafka server address
kafka_hosts = "localhost:9092"  
# topic name
topic_name = "hicham_topic"

# Kafka client
client = KafkaClient(hosts=kafka_hosts)
# specifie the topic
topic = client.topics[topic_name]
# specifie the consumer
consumer = topic.get_simple_consumer()

# Consume messages from the Kafka topic
for message in consumer:
    if message is not None:
        data = json.loads(message.value.decode('utf-8'))
        # Process the data as needed
        print(data)

# Close the consumer connection (optional)
consumer.stop()