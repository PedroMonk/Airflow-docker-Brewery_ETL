"""
Gold Layer Module - Open Brewery DB ETL Pipeline

This module handles the transformation of data from the Silver layer to the Gold layer
following the medallion architecture. It performs data aggregation and creates a business-level
view of brewery counts by type and location.

The module performs the following operations:
1. Reads data from the Silver layer
2. Validates required columns
3. Aggregates data by brewery type and location
4. Creates count metrics
5. Partitions and stores data in the Gold layer

Author: [Your Name]
Date: November 2024
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from pyspark.sql.types import (
   StructType,
   StructField,
   StringType,
   LongType,
   FloatType,
   ArrayType
)
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
   """
   Creates and returns a configured Spark session for S3 access.
   
   Returns:
       SparkSession: Configured Spark session
       
   Raises:
       ValueError: If AWS credentials are not found
   """
   try:
       logger.info("Starting Spark session creation for Gold transformation")
       
       # Get AWS credentials
       aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
       aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
       aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

       if not aws_access_key or not aws_secret_key:
           raise ValueError("AWS credentials not found in environment variables.")

       # Define compatible versions
       hadoop_aws_version = "3.3.1"        # Must match Silver layer
       aws_java_sdk_version = "1.12.375"   # Must match Silver layer

       # Create Spark session with necessary dependencies
       spark = SparkSession.builder \
           .appName("TransformGoldOpenBreweryDB") \
           .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
           .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
           .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
           .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
           .config("spark.hadoop.fs.s3a.path.style.access", "true") \
           .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},com.amazonaws:aws-java-sdk-bundle:{aws_java_sdk_version}") \
           .getOrCreate()
       
       logger.info("Spark session created successfully for Gold transformation")
       return spark
   except Exception as e:
       logger.error(f"Error creating Spark session for Gold transformation: {e}")
       raise

def read_silver_data(spark, s3_bucket, s3_silver_path, execution_date):
   """
   Reads data from the Silver layer partitioned by location and date.
   
   Args:
       spark: SparkSession object
       s3_bucket: S3 bucket name
       s3_silver_path: Path to Silver layer in S3
       execution_date: Date of execution
       
   Returns:
       DataFrame: Data from Silver layer with required columns
   """
   try:
       logger.info(f"Reading data from Silver layer: s3a://{s3_bucket}/{s3_silver_path}/date={execution_date}/")
       
       # Define base path and read data
       base_path = f"s3a://{s3_bucket}/{s3_silver_path}/date={execution_date}"
       
       # Read data directly without partition inference
       df_silver = spark.read \
           .format("parquet") \
           .load(base_path)
       
       # Select only required columns
       df_silver = df_silver.select(
           "brewery_type",
           "localizacao"
       )

       record_count = df_silver.count()
       logger.info(f"Silver layer data loaded: {record_count} records")
       
       # Show schema and sample data for debugging
       logger.info("DataFrame Schema:")
       df_silver.printSchema()
       
       logger.info("Data Sample:")
       df_silver.show(5, False)
       
       return df_silver
   except Exception as e:
       logger.error(f"Error reading data from Silver layer: {e}")
       raise

def aggregate_data(df):
   """
   Aggregates data to calculate brewery counts by type and location.
   
   Args:
       df: Input DataFrame from Silver layer
       
   Returns:
       DataFrame: Aggregated data with brewery counts
       
   Raises:
       ValueError: If required columns are missing
   """
   try:
       logger.info("Starting data aggregation for Gold layer")
       
       # Validate required columns
       logger.info("Validating brewery_type and location columns")
       for column in ["brewery_type", "localizacao"]:
           if column not in df.columns:
               raise ValueError(f"Column {column} not found in DataFrame")
           
           null_count = df.filter(col(column).isNull()).count()
           if null_count > 0:
               logger.warning(f"Found {null_count} records with null values in {column}")

       # Show sample data before aggregation
       logger.info("Data sample before aggregation:")
       df.select("brewery_type", "localizacao").show(5, False)
       
       # Perform aggregation
       df_gold = df.groupBy("brewery_type", "localizacao") \
                   .agg(count("*").alias("quantity_of_breweries"))
       
       # Show aggregation results
       logger.info("Data sample after aggregation:")
       df_gold.show(5, False)
       
       # Count total records after aggregation
       total_records = df_gold.count()
       logger.info(f"Total records after aggregation: {total_records}")
       
       return df_gold
   except Exception as e:
       logger.error(f"Error during data aggregation: {e}")
       raise

def write_gold_data(df, spark, s3_bucket, s3_gold_path, execution_date):
   """
   Writes aggregated data to the Gold layer with partitioning.
   
   Args:
       df: DataFrame to write
       spark: SparkSession object
       s3_bucket: S3 bucket name
       s3_gold_path: Path to Gold layer in S3
       execution_date: Date of execution for partitioning
   """
   try:
       logger.info(f"Writing data to Gold layer: s3a://{s3_bucket}/{s3_gold_path}/date={execution_date}/")
       
       # Define output path with date partition
       output_path = f"s3a://{s3_bucket}/{s3_gold_path}/date={execution_date}/"
       
       # Write data with partitioning
       df.write.mode('overwrite') \
           .partitionBy("brewery_type", "localizacao") \
           .parquet(output_path)
       logger.info("Data successfully written to Gold layer with brewery_type and location partitioning")
   except Exception as e:
       logger.error(f"Error writing data to Gold layer: {e}")
       raise

def transform_gold(**kwargs):
   """
   Main function to transform data from Silver to Gold layer.
   
   This function:
   1. Sets up environment and configurations
   2. Reads data from Silver layer
   3. Performs data aggregation
   4. Writes aggregated data to Gold layer
   
   Args:
       **kwargs: Keyword arguments from Airflow
   """
   logger.info("Starting transform_gold task")
   spark = None
   try:
       # Get environment variables
       s3_bucket = os.getenv('S3_BUCKET')
       s3_silver_path = os.getenv('S3_SILVER_PATH')
       s3_gold_path = os.getenv('S3_GOLD_PATH')
       
       if not s3_bucket or not s3_silver_path or not s3_gold_path:
           logger.error("S3_BUCKET, S3_SILVER_PATH or S3_GOLD_PATH environment variables not set")
           raise ValueError("Required S3 environment variables not set")

       # Get execution date
       execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
       logger.info(f"Execution date: {execution_date}")

       # Create Spark session
       spark = create_spark_session()

       # Read Silver data
       df_silver = read_silver_data(spark, s3_bucket, s3_silver_path, execution_date)

       # Aggregate data for Gold layer
       df_gold = aggregate_data(df_silver)

       # Write to Gold layer
       write_gold_data(df_gold, spark, s3_bucket, s3_gold_path, execution_date)
       
       logger.info("transform_gold task completed successfully")
   except Exception as e:
       logger.error(f"Error in transform_gold task: {e}")
       raise
   finally:
       if spark:
           spark.stop()
           logger.info("Spark session terminated for transform_gold task")

            