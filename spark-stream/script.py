import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import from_json, current_date, datediff, from_utc_timestamp, col, to_date, round
from pyspark.conf import SparkConf
from cassandra.cluster import Cluster
SparkSession.builder.config(conf=SparkConf())

def connect_to_cassandra(host,port):
    try:
        # provide contact points
        cluster = Cluster([host],port=port)
        session = cluster.connect()
        print("Connection established successfully.")
        return session
    except:
        print("Connection failed.")

def create_cassandra_keyspace(session,keyspaceName):
    try:
        create_keyspace_query = """ CREATE KEYSPACE IF NOT EXISTS """+keyspaceName+ \
        """ WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"""
        session.execute(create_keyspace_query)
        print("Keyspace was created successfully.")
    except:
        print(f"Error in creating keyspace {keyspaceName}.")

def create_cassandra_table(session,tableName):
    try:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {tableName} (
            id UUID PRIMARY KEY,
            gender TEXT,
            title TEXT,
            fullname TEXT,
            username TEXT,
            email TEXT,
            phone TEXT,
            fulladdress TEXT,
            nationality TEXT,
            birthday TEXT,
            age INT,
            inscription TEXT
        )
        """
        session.execute(create_table_query)
        print("table was created successfully.")
    except:
        print(f"Error in creating table {tableName}.")

def save_to_cassandra(df, keyspacename, tablename):
    # Save the DataFrame to Cassandra
    result_df_clean.writeStream \
        .outputMode("append") \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "./checkpoint/data") \
        .option("keyspace", keyspacename) \
        .option("table", tablename) \
        .start()

    # Start the streaming query
    query = result_df_clean.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("format", "json") \
        .start()

    # Wait for the query to terminate
    query.awaitTermination()

# Create a session
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config('spark.cassandra.connection.host', 'localhost')\
    .getOrCreate()
    # .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\

# Read the stream comming from kafka
df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "hicham_topic") \
  .load()

# Cast data as key value pair
spark_df = df.selectExpr("CAST(value AS STRING)")

# Define the schema of our json data
schema = StructType([
    StructField("results", 
                ArrayType(
                    StructType([
                    StructField("gender", StringType(), nullable=True),
                    StructField("name", 
                                StructType([
                                    StructField("title", StringType(), nullable=True),
                                    StructField("first", StringType(), nullable=True),
                                    StructField("last", StringType(), nullable=True)
                                ]),
                                nullable=True),
                    StructField("location", 
                                StructType([
                                    StructField("street", 
                                                StructType([
                                                    StructField("number", IntegerType(), nullable=True),
                                                    StructField("name", StringType(), nullable=True)
                                                ]),
                                                nullable=True),
                                    StructField("city", StringType(), nullable=True),
                                    StructField("state", StringType(), nullable=True),
                                    StructField("country", StringType(), nullable=True),
                                    StructField("postcode", IntegerType(), nullable=True),
                                ]),
                                nullable=True),
                    StructField("email", StringType(), nullable=True),
                    StructField("login", 
                                StructType([
                                    StructField("uuid", StringType(), nullable=True),
                                    StructField("username", StringType(), nullable=True),
                                ]),
                                nullable=True),
                    StructField("dob", 
                                StructType([
                                    StructField("date", StringType(), nullable=True),
                                    StructField("age", IntegerType(), nullable=True)
                                ]),
                                nullable=True),
                    StructField("registered", 
                                StructType([
                                    StructField("date", StringType(), nullable=True),
                                    StructField("age", IntegerType(), nullable=True)
                                ]),
                                nullable=True),
                    StructField("phone", StringType(), nullable=True),
                    StructField("cell", StringType(), nullable=True),
                    StructField("nat", StringType(), nullable=True)
                ]),True),
            nullable=True
            ),
])

# Apply schema on data
spark_df_extended = spark_df.withColumn("jsonData", from_json(spark_df["value"], schema))

# Select our fields
result_df = spark_df_extended.selectExpr(
    "jsonData.results.login.uuid[0] as id",
    "jsonData.results.gender[0] as gender",
    "jsonData.results.name.title[0] as title",
    "concat(jsonData.results.name.first[0], ' ', jsonData.results.name.last[0]) as fullname",
    "jsonData.results.login.username [0] as username",
    "jsonData.results.email[0] as email",
    "jsonData.results.phone[0] as phone",
    "concat(jsonData.results.location.street.number[0], ', ', jsonData.results.location.street.name[0], \
    ', ', jsonData.results.location.city[0], ', ', jsonData.results.location.state[0], ', ', jsonData.results.location.country[0],\
    ', ', jsonData.results.location.postcode[0]) as fulladdress",
    "jsonData.results.nat[0] as nationality",
    "jsonData.results.dob.date[0] as birthday",
    "jsonData.results.registered.date[0] as inscription",
)

# Calculate the age in years based on the the birthday date
result_df = result_df.withColumn("age", round(datediff(current_date(), to_date(result_df["birthday"])) / 365).cast("integer"))

print("-------------------- Connect to Cassandra -----------------------")
cassandra_host = 'localhost'
cassandra_port = 9042
keyspaceName = 'hicham_keyspace'
tableName = 'user_table'


session = connect_to_cassandra(cassandra_host,cassandra_port)
create_cassandra_keyspace(session,keyspaceName)
session.set_keyspace(keyspaceName)
create_cassandra_table(session,tableName)

# Apply filters to respect the RGPD
result_df_clean = result_df.filter("id IS NOT NULL and age >= 18")

# Insert data to cassandra
save_to_cassandra(result_df_clean, keyspaceName, tableName)
