import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql.types import StructType, StructField, StringType

# Step 1: Set up the Cassandra connection and create the keyspace and table
def setup_cassandra():
    cluster = Cluster(['localhost'])  # Specify the IP addresses of your Cassandra nodes
    session = cluster.connect()

    if session is not None:
        # Create a keyspace
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS kafka_data
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)

        print("Keyspace created successfully!")

        # Create a table
        session.execute("""
            CREATE TABLE IF NOT EXISTS kafka_data.created_users (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """)
    
        print("Table created successfully!")

# Step 2: Initialize Spark Session
def get_spark_session():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('KafkaToCassandra') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

# Step 3: Read from Kafka
def read_from_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

# Step 4: Write to Cassandra
def write_to_cassandra(df):
    
    Streaming_query = (df.writeStream.format("org.apache.spark.sql.cassandra")
    .option("keyspace", "kafka_data")
    .option("table", "created_users")
    .option('checkpointLocation', '/tmp/checkpoint')
    .start())
        
    Streaming_query.awaitTermination()

# Main Function
if __name__ == "__main__":
    
    # create spark connection
    spark_conn = get_spark_session()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = read_from_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = setup_cassandra()
        
        logging.info("Streaming is being started...")

        write_to_cassandra(selection_df)
