import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import from_json
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
            firstname TEXT,
            lastname TEXT,
            username TEXT,
            email TEXT,
            phone TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            birthday TEXT,
            inscription TEXT
        )
        """
        session.execute(create_table_query)
        print("table was created successfully.")
    except:
        print(f"Error in creating table {tableName}.")


# Create a session
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config('spark.cassandra.connection.host', 'localhost')\
    .getOrCreate()

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
                                    StructField("coordinates", 
                                                StructType([
                                                    StructField("latitude", StringType(), nullable=True),
                                                    StructField("longitude", StringType(), nullable=True)
                                                ]),
                                                nullable=True),
                                    StructField("timezone", 
                                                StructType([
                                                    StructField("offset", StringType(), nullable=True),
                                                    StructField("description", StringType(), nullable=True)
                                                ]),
                                                nullable=True)
                                ]),
                                nullable=True),
                    StructField("email", StringType(), nullable=True),
                    StructField("login", 
                                StructType([
                                    StructField("uuid", StringType(), nullable=True),
                                    StructField("username", StringType(), nullable=True),
                                    StructField("password", StringType(), nullable=True),
                                    StructField("salt", StringType(), nullable=True),
                                    StructField("md5", StringType(), nullable=True),
                                    StructField("sha1", StringType(), nullable=True),
                                    StructField("sha256", StringType(), nullable=True)
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
                    StructField("id", 
                                StructType([
                                    StructField("name", StringType(), nullable=True),
                                    StructField("value", StringType(), nullable=True)
                                ]),
                                nullable=True),
                    StructField("picture", 
                                StructType([
                                    StructField("large", StringType(), nullable=True),
                                    StructField("medium", StringType(), nullable=True),
                                    StructField("thumbnail", StringType(), nullable=True)
                                ]),
                                nullable=True),
                    StructField("nat", StringType(), nullable=True)
                ]),True),
            nullable=True
            ),
])

# Apply schema on data
spark_df_extended = spark_df.withColumn("jsonData", from_json(spark_df["value"], schema))

# Select some fields
result_df = spark_df_extended.selectExpr(
    # "explode(jsonData.results) as result",
    "jsonData.results.login.uuid[0] as id",
    "jsonData.results.gender[0] as gender",
    "jsonData.results.name.title[0] as title",
    "jsonData.results.name.first[0] as firstname",
    "jsonData.results.name.last[0] as lastname",
    "jsonData.results.login.username [0] as username",
    "jsonData.results.email[0] as email",
    "jsonData.results.phone[0] as phone",
    "jsonData.results.location.city[0] as city",
    "jsonData.results.location.state[0] as state",
    "jsonData.results.location.country[0] as country",
    "jsonData.results.dob.date[0] as birthday",
    "jsonData.results.registered.date[0] as inscription",
    # "jsonData.results.login.password[0] as password",
    # "jsonData.results.location.street.number",
    # "jsonData.results.location.street.name",
    # "jsonData.results.location.postcode",
    # "jsonData.results.location.coordinates.latitude",
    # "jsonData.results.location.coordinates.longitude",
    # "jsonData.results.location.timezone.offset",
    # "jsonData.results.location.timezone.description",
    # "jsonData.results.login.salt",
    # "jsonData.results.login.md5",
    # "jsonData.results.login.sha1",
    # "jsonData.results.login.sha256",
    # "jsonData.results.dob.age",
    # "jsonData.results.registered.age",
    # "jsonData.results.cell",
    # "jsonData.results.id.name",
    # "jsonData.results.id.value",
    # "jsonData.results.picture.large",
    # "jsonData.results.picture.medium",
    # "jsonData.results.picture.thumbnail",
    # "jsonData.results.nat"
)


print("-------------------- Connect to Cassandra -----------------------")
cassandra_host = 'localhost'
cassandra_port = 9042
keyspaceName = 'hicham_keyspace'
tableName = 'user_table'


session = connect_to_cassandra(cassandra_host,cassandra_port)
create_cassandra_keyspace(session,keyspaceName)
session.set_keyspace(keyspaceName)
create_cassandra_table(session,tableName)

result_df_clean = result_df.filter("id IS NOT NULL")
# Save the DataFrame to Cassandra
result_df_clean.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "./checkpoint/data") \
    .option("keyspace", keyspaceName) \
    .option("table", tableName) \
    .start()

# Start the streaming query
query = result_df_clean.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("format", "json") \
    .start()

# Wait for the query to terminate
query.awaitTermination()

# Connect to cassandra
# cassandra = SparkSession.builder \
#     .appName("CassandraKeyspaceCreation") \
#     .config("spark.cassandra.connection.host", "localhost") \
#     .config("spark.cassandra.connection.port", "9042") \
#     .getOrCreate()

# # print(cassandra)
# cassandra.sql("CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")


# -------------------------------------------------------------------------------
# Save data to Cassandra
# query = result_df.writeStream \
#     .outputMode("append") \
#     .format("org.apache.spark.sql.cassandra") \
#     .option("keyspace", "hicham_keyspace") \
#     .option("table", "hicham_table") \
#     .option("spark.cassandra.connection.host", "localhost") \
#     .option("spark.cassandra.connection.port", "9042") \
#     .start()

# query.awaitTermination()

# # Set up Cassandra connector
# cassandraConfig = {
#     "keyspace": "your_keyspace_name",  # Replace with your Cassandra keyspace
#     "table": "your_table_name",  # Replace with your Cassandra table
#     "spark.cassandra.connection.host": "localhost",  # Cassandra host address
#     "spark.cassandra.connection.port": "9042"  # Cassandra port
# }

# # Save transformed data to Cassandra
# transformedStream.saveToCassandra(**cassandraConfig)

# # Start the streaming context
# ssc.start()
# ssc.awaitTermination()

