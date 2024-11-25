# spark_timeseries_analysis.py
import os
import sys
import urllib.request
from pathlib import Path
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.window import Window
import pandas as pd
from pyspark.sql import DataFrame
from cassandra.cluster import Cluster

def setup_hadoop_binaries():
    """Download and setup Hadoop binaries for Windows"""
    hadoop_dir = Path("C:/hadoop")
    bin_dir = hadoop_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    # Download winutils.exe if not present
    winutils_path = bin_dir / "winutils.exe"
    if not winutils_path.exists():
        print("Downloading winutils.exe...")
        winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe"
        urllib.request.urlretrieve(winutils_url, str(winutils_path))

    # Download hadoop.dll if not present
    hadoop_dll_path = bin_dir / "hadoop.dll"
    if not hadoop_dll_path.exists():
        print("Downloading hadoop.dll...")
        hadoop_dll_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/hadoop.dll"
        urllib.request.urlretrieve(hadoop_dll_url, str(hadoop_dll_path))

    # Set environment variables
    os.environ['HADOOP_HOME'] = str(hadoop_dir)
    os.environ['PATH'] = f"{str(bin_dir)};{os.environ['PATH']}"
    
    print("Hadoop binaries setup completed")

def create_spark_session():
    """Create Spark session with necessary configurations"""
    from pyspark.sql import SparkSession
    
    return SparkSession.builder \
        .appName("StockTimeSeriesAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .master("local[*]") \
        .getOrCreate()

def prepare_features(df):
    """Prepare time series features from the data"""
    # Create window specifications for feature creation
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")
    
    return df \
        .withColumn("price_lag1", lag("price", 1).over(window_spec)) \
        .withColumn("price_lag2", lag("price", 2).over(window_spec)) \
        .withColumn("price_lag3", lag("price", 3).over(window_spec)) \
        .withColumn("volume_lag1", lag("volume", 1).over(window_spec)) \
        .withColumn("price_change", col("price") - col("price_lag1")) \
        .withColumn("volume_change", col("volume") - col("volume_lag1")) \
        .withColumn("hour", hour("timestamp")) \
        .withColumn("day", dayofmonth("timestamp")) \
        .withColumn("day_of_week", dayofweek("timestamp")) \
        .withColumn("rolling_mean_price", 
                    avg("price").over(window_spec.rowsBetween(-5, 0))) \
        .withColumn("rolling_mean_volume", 
                    avg("volume").over(window_spec.rowsBetween(-5, 0))) \
        .na.drop()

def train_model(df):
    """Train time series prediction model"""
    # Define features for the model
    feature_cols = [
        "price_lag1", "price_lag2", "price_lag3",
        "volume_lag1", "price_change", "volume_change",
        "hour", "day", "day_of_week",
        "rolling_mean_price", "rolling_mean_volume"
    ]
    
    # Prepare feature vector
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )
    
    # Prepare data
    model_df = assembler.transform(df)
    train_df, test_df = model_df.randomSplit([0.8, 0.2])
    
    # Train Random Forest model
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="price",
        numTrees=50,
        maxDepth=10
    )
    
    model = rf.fit(train_df)
    prediction = model.transform(test_df)
    
    # Evaluate model
    evaluator = RegressionEvaluator(
        labelCol="price",
        predictionCol="prediction",
        metricName="rmse"
    )
    rmse = evaluator.evaluate(prediction)
    
    return model, rmse, prediction

def process_batch(df, epoch_id):
    """Process each batch of data with time series analysis"""
    try:
        if df.rdd.isEmpty():
            print(f"Batch {epoch_id}: No data received")
            return
        
        print(f"\nProcessing batch {epoch_id}")
        
        # Prepare features
        feature_df = prepare_features(df)
        
        # Train model if enough data
        if feature_df.count() > 10:
            model, rmse, prediction = train_model(feature_df)
            
            print(f"\nBatch {epoch_id} Results:")
            print(f"RMSE: {rmse:.2f}")
            
            # Show latest prediction
            print("\nLatest prediction vs Actual:")
            prediction.select("timestamp", "symbol", "price", "prediction") \
                .orderBy(desc("timestamp")) \
                .limit(5) \
                .show()
            
            # Calculate prediction metrics
            prediction = prediction.withColumn(
                "prediction_error", 
                abs(col("prediction") - col("price")) / col("price") * 100
            )
            
            print("\nPrediction Error Statistics:")
            prediction.select(
                mean("prediction_error").alias("mean_error_percentage"),
                max("prediction_error").alias("max_error_percentage"),
                min("prediction_error").alias("min_error_percentage")
            ).show()
            
            # Save results to Cassandra
            save_to_cassandra(prediction, 'stock_analysis', 'prediction')
        
        else:
            print(f"Batch {epoch_id}: Not enough data for training (minimum 10 records needed)")
            
    except Exception as e:
        print(f"Error processing batch {epoch_id}: {str(e)}")




def save_to_cassandra(df: DataFrame, keyspace: str, table: str):
    # Connect to the Cassandra cluster
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect(keyspace)

    # Create the table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        symbol text,
        timestamp timestamp,
        prediction float,
        price float,
        volume int,
        PRIMARY KEY (timestamp, symbol)
    )"""
    session.execute(create_table_query)

    # Insert data
    insert_query = session.prepare(f"INSERT INTO {table} (timestamp, symbol, prediction, price, volume) VALUES (?, ?, ?, ?, ?)")

    # Use a batch for efficiency
    for row in df.collect():
        session.execute(insert_query, (row.timestamp, row.symbol, row.prediction, row.price, int(row.volume)))

    print(f"Data saved to table {table} under keyspace {keyspace}.")

def start_streaming():
    """Start the streaming process"""
    try:
        # Setup environment
        setup_hadoop_binaries()
        
        # Create Spark session
        print("Creating Spark session...")
        spark = create_spark_session()
        print("Spark session created successfully")
        
        # Define schema for incoming data
        schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("volume", DoubleType(), True)
        ])
        
        # Read from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "stock_data") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Process the stream
        query = parsed_df \
            .writeStream \
            .foreachBatch(process_batch) \
            .outputMode("update") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        print("Started streaming. Waiting for data...")
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error in streaming: {str(e)}")
        raise

if __name__ == "__main__":
    start_streaming()