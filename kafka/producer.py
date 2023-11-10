import requests
from kafka import KafkaProducer
import json
import time


# Fake user data for test
# user = {"results":[{"gender":"male","name":{"title":"Monsieur","first":"Steve","last":"Hubert"},"location":{"street":{"number":3218,"name":"Rue Abel-Hovelacque"},"city":"Losone","state":"Basel-Landschaft","country":"Switzerland","postcode":2945,"coordinates":{"latitude":"-59.7173","longitude":"-156.0588"},"timezone":{"offset":"+9:00","description":"Tokyo, Seoul, Osaka, Sapporo, Yakutsk"}},"email":"steve.hubert@example.com","login":{"uuid":"e7d55813-c682-4592-9153-68473fbcd026","username":"goldenelephant453","password":"angus1","salt":"VAHivLkY","md5":"99c382a23e38a425f922936f27409108","sha1":"026af30c1933a4b2616405a0f2568b7c82d2bba2","sha256":"4986de6916e29dd36036562569d729cd70d4cee05b37cb959a5a002d0b6a1a93"},"dob":{"date":"1973-08-26T17:43:16.339Z","age":50},"registered":{"date":"2008-05-08T13:11:26.627Z","age":15},"phone":"079 788 01 41","cell":"079 103 37 29","id":{"name":"AVS","value":"756.8120.9149.50"},"picture":{"large":"https://randomuser.me/api/portraits/men/46.jpg","medium":"https://randomuser.me/api/portraits/med/men/46.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/men/46.jpg"},"nat":"CH"}],"info":{"seed":"17ac51d971a5d813","results":1,"page":1,"version":"1.4"}}

# Kafka server address
kafka_host = "localhost:9092"  
# Topic name
topic_name = "hicham_topic"

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_host)

while True:
    # specify the url of the api
    response = requests.get("https://randomuser.me/api/")
    # Serialize the user object to JSON
    data = json.dumps(response.json())

    # data = json.dumps(user)
    if data:
        # Produce the data to Kafka
        producer.send(topic_name, value=data.encode('utf-8'))
        print("Send user data to spark. Wait 5 seconds..")
    else:
        print("Empty data. Skipping sending to spark.")
        
    # Delay for 5 seconds before sending the next message
    time.sleep(5)

# # Close the producer connection (optional)
# producer.stop()