# Development of a Real-Time Data Pipeline for User Profile Analysis

## Project description 

Using Kafka and PySpark, we need to efficiently transform, aggregate, and store user data generated by the randomuser.me API. We should save the transformed data into a Cassandra table, and for the aggregated or analyzed data, we should store it in a MongoDB collection.

## Technologies

<img src="https://github.com/Tarifi-Hicham/user_analysis_spark_kafka/assets/125143059/11f26e5a-2196-43d5-9246-9193555c2fe7" alt="Image" width="200" height="150">
<img src="https://github.com/Tarifi-Hicham/user_analysis_spark_kafka/assets/125143059/d79eb1ed-f841-4abf-86d2-f86188ee8a45" alt="Image" width="200" height="150">
<img src="https://github.com/Tarifi-Hicham/user_analysis_spark_kafka/assets/125143059/a0482ded-0365-49c7-8900-b0ab96d951b8" alt="Image" width="200" height="150">
<img src="https://github.com/Tarifi-Hicham/user_analysis_spark_kafka/assets/125143059/07021314-ece8-4d6f-a6e7-89378fddb1ff" alt="Image" width="200" height="150">

## Pipeline of this project

<img src="https://github.com/Tarifi-Hicham/user_analysis_spark_kafka/assets/125143059/4ce75a17-9f2b-45be-b2bb-e110e4ac6e32" alt="Image" width="720" height="300">

## API

The RandomUser API is a public API that provides randomly generated user data. It allows developers to retrieve fictitious user information such as names, addresses, email addresses, phone numbers, and profile images. The API is commonly used in software development for various purposes, including prototyping, testing, and generating sample data.

Here's an example of the JSON structure returned by the RandomUser API for a single user:
```
{
  "results": [
    {
      "gender": "female",
      "name": {
        "title": "Ms",
        "first": "Emily",
        "last": "Smith"
      },
      "email": "emily.smith@example.com",
      "username": "crazyswan123",
      "password": "9s8d7f6g",
      "dob": {
        "date": "1990-05-25",
        "age": 33
      },
      "address": {
        "street": "123 Main St",
        "city": "New York",
        "state": "NY",
        "postcode": "10001",
        "country": "United States"
      },
      "phone": "(123) 456-7890",
      "picture": {
        "large": "https://randomuser.me/api/portraits/women/1.jpg",
        "medium": "https://randomuser.me/api/portraits/med/women/1.jpg",
        "thumbnail": "https://randomuser.me/api/portraits/thumb/women/1.jpg"
      }
    }
  ],
  "info": {
    "seed": "abc123",
    "results": 1,
    "page": 1,
    "version": "1.3"
  }
}
```

# Real-time Data Processing with PySpark, Cassandra, and MongoDB

This repository contains a PySpark script that demonstrates real-time data processing using Apache Kafka, PySpark, Cassandra, and MongoDB. The script reads streaming data from a Kafka topic, applies transformations and filters using PySpark, and saves the processed data to both Cassandra and MongoDB.

## Prerequisites

Before running the script, make sure you have the following dependencies installed:

- <a href='https://kafka.apache.org/downloads'>Apache Kafka</a>
- <a href='https://spark.apache.org/docs/latest/api/python/getting_started/install.html'>PySpark</a>
- <a href='https://cassandra.apache.org/_/download.html'>Cassandra</a>
- <a href='https://www.mongodb.com/try/download/community'>MongoDB</a>

## Script Overview

The script performs the following steps:

1. Connects to Cassandra and creates a keyspace and table to store the processed data.
2. Connects to MongoDB and specifies the database and collection to store the processed data.
3. Reads streaming data from a Kafka topic using PySpark.
4. Applies a schema to the JSON data received from Kafka.
5. Selects the required fields from the JSON data and applies transformations.
6. Encrypts sensitive data (phone number and full address) using `bcrypt`.
7. Calculates the age in years based on the birthday date.
8. Applies filters to respect data privacy regulations (`GDPR`).
9. Saves the processed data to Cassandra.
10. Saves a subset of the processed data to MongoDB.

## Configuration

Before running the script, make sure to configure the following settings:

- Kafka broker address: Update the `kafka.bootstrap.servers` option in the script to point to your Kafka broker.
- Cassandra connection details: Update the `cassandra_host` and `cassandra_port` variables in the script to match your Cassandra cluster.
- Cassandra keyspace and table: Update the `keyspaceName` and `tableName` variables in the script to specify the desired keyspace and table names in Cassandra.
- MongoDB connection details: Update the `mongo_uri`, `mongo_db`, and `mongo_collection` variables in the script to match your MongoDB setup.

## Running the Script

To run the script:

1. Start Zookeeper by running this command
   ```bash
   path\to\kafka\config> zookeeper-server-start.bat zookeeper.properties
3. Start the Kafka broker.
   ```bash
   path\to\kafka\config> kafka-server-start.bat server.properties
5. Make sure Cassandra is running.
6. Make sure MongoDB is running.
7. Execute the script using PySpark:
   ```bash
   spark-submit spark-stream/script.py

## GDPR

the following steps are taken to respect the General Data Protection Regulation (GDPR):

Encryption of Sensitive Data: To protect the privacy of sensitive information, such as phone numbers and full addresses, the script uses the bcrypt library to encrypt these fields. The encrypt_data user-defined function (UDF) applies bcrypt hashing to the data, making it irreversible. This ensures that even if the data is compromised, the original sensitive information cannot be easily recovered.

Age Calculation and Filter: The script calculates the age of each record based on the provided birthday date. The datediff and round functions are used to compute the age in years. After calculating the age, a filter is applied to retain only records where the age is 18 or above. This filtering ensures that only data belonging to individuals who are legally considered adults is processed and stored.

By encrypting sensitive data and applying age-based filters, the script aims to adhere to the GDPR requirements for data privacy and protection. These measures help ensure that personally identifiable information (PII) is handled securely and that data processing respects the rights and privacy of individuals.
