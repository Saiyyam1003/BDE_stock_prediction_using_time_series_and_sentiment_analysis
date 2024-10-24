import os
import logging
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with necessary configurations"""
    return SparkSession.builder \
        .appName("StoreToCassandra") \
        .master("local[*]") \
        .getOrCreate()

def save_to_cassandra(df, keyspace, table):
    """Save DataFrame to Cassandra"""
    try:
        # Create Cassandra connection
        cluster = Cluster(['localhost'])  # Adjust if your Cassandra is hosted differently
        session = cluster.connect(keyspace)

        # Prepare insert statement
        insert_stmt = SimpleStatement(f"""
            INSERT INTO {table} (timestamp, symbol, price, prediction)
            VALUES (?, ?, ?, ?)
        """)

        for row in df.collect():
            session.execute(insert_stmt, (row.timestamp, row.symbol, row.price, row.prediction))

        logger.info(f"Successfully saved data to {keyspace}.{table}")

    except Exception as e:
        logger.error(f"Error saving to Cassandra: {str(e)}")
    finally:
        cluster.shutdown()

def start_spark_streaming_and_save():
    """Start the Spark session and read from the stream to save into Cassandra"""
    spark = create_spark_session()

    # Define schema for reading predictions
    schema = "timestamp TIMESTAMP, symbol STRING, price DOUBLE, prediction DOUBLE"

    # Read the processed predictions from a specified location or stream
    predictions_df = spark.readStream \
        .format("parquet") \
        .load("path/to/your/predictions")  # Specify the path where your predictions are saved

    # Process each batch to save to Cassandra
    query = predictions_df \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: save_to_cassandra(df, "your_keyspace", "your_table")) \
        .outputMode("update") \
        .start()

    logger.info("Started streaming predictions to Cassandra. Waiting for data...")
    query.awaitTermination()

if __name__ == "__main__":
    start_spark_streaming_and_save()
