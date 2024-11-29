"""
Silver Layer Module - Open Brewery DB ETL Pipeline

This module handles the transformation of data from the Bronze layer to the Silver layer
following the medallion architecture. It performs data cleaning, type conversion, and
partitioning of the data.

The module performs the following operations:
1. Reads data from the Bronze layer
2. Removes duplicates
3. Handles null values
4. Converts data types
5. Creates location column
6. Partitions data by location and date
7. Saves transformed data to the Silver layer

Author: [Pcosta]
Date: November 2024
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, lit
from pyspark.sql.types import FloatType
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
       logger.info("Starting Spark session creation for Silver transformation")
       # Get AWS credentials
       aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
       aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
       aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

       if not aws_access_key or not aws_secret_key:
           raise ValueError("AWS credentials not found in environment variables.")

       # Define compatible versions
       hadoop_aws_version = "3.3.1"        # Must match Bronze layer
       aws_java_sdk_version = "1.12.375"   # Must match Bronze layer

       # Create Spark session with necessary dependencies
       spark = SparkSession.builder \
           .appName("TransformSilverOpenBreweryDB") \
           .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
           .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
           .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
           .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
           .config("spark.hadoop.fs.s3a.path.style.access", "true") \
           .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},com.amazonaws:aws-java-sdk-bundle:{aws_java_sdk_version}") \
           .getOrCreate()
       
       logger.info("Spark session created successfully for Silver transformation")
       return spark
   except Exception as e:
       logger.error(f"Error creating Spark session for Silver transformation: {e}")
       raise

def read_bronze_data(spark, s3_bucket, s3_bronze_path, execution_date):
   """
   Reads data from the Bronze layer.
   
   Args:
       spark: SparkSession object
       s3_bucket: S3 bucket name
       s3_bronze_path: Path to Bronze layer in S3
       execution_date: Date of execution
       
   Returns:
       DataFrame: Raw data from Bronze layer
   """
   try:
       logger.info(f"Reading data from Bronze layer: s3a://{s3_bucket}/{s3_bronze_path}/date={execution_date}")
       df_bronze = spark.read.parquet(f"s3a://{s3_bucket}/{s3_bronze_path}/date={execution_date}")
       record_count = df_bronze.count()
       logger.info(f"Bronze layer data loaded: {record_count} records")
       return df_bronze
   except Exception as e:
       logger.error(f"Error reading data from Bronze layer: {e}")
       raise

def transform_data(df):
   """
   Performs data transformations for the Silver layer.
   
   Transformations include:
   - Removing duplicates based on ID
   - Handling null values
   - Converting data types
   - Creating location column
   
   Args:
       df: Input DataFrame from Bronze layer
       
   Returns:
       DataFrame: Transformed data for Silver layer
   """
   try:
       logger.info("Starting Silver layer transformations")

       # Remove duplicates based on 'id'
       df = df.dropDuplicates(["id"])
       record_count = df.count()
       logger.info(f"Duplicates removed. Total records: {record_count}")

       # Handle null values
       df = df.withColumn("phone", when(col("phone").isNull(), "N/A").otherwise(col("phone")))

       # Convert data types
       df = df.withColumn("longitude", col("longitude").cast(FloatType()))
       df = df.withColumn("latitude", col("latitude").cast(FloatType()))

       # Add location column if not exists
       if "localizacao" not in df.columns:
           df = df.withColumn("localizacao", concat_ws(", ", col("city"), col("state")))
           logger.info("Location column added to DataFrame")
       else:
           logger.info("Location column already exists in DataFrame")

       logger.info("Data types converted successfully and location column ensured")
       logger.info("Silver layer transformations completed")
       return df
   except Exception as e:
       logger.error(f"Error during Silver layer transformations: {e}")
       raise

def write_silver_data(df, spark, s3_bucket, s3_silver_path, execution_date):
   """
   Writes transformed data to the Silver layer with partitioning.
   
   Args:
       df: DataFrame to write
       spark: SparkSession object
       s3_bucket: S3 bucket name
       s3_silver_path: Path to Silver layer in S3
       execution_date: Date of execution for partitioning
   """
   try:
       logger.info(f"Writing data to Silver layer: s3a://{s3_bucket}/{s3_silver_path}/date={execution_date}")
       # Add date column
       df = df.withColumn("date", lit(execution_date))

       # Partition by location and date
       df.write.mode('overwrite') \
           .partitionBy("localizacao", "date") \
           .parquet(f"s3a://{s3_bucket}/{s3_silver_path}/date={execution_date}")
       logger.info("Data successfully written to Silver layer with location and date partitioning")
   except Exception as e:
       logger.error(f"Error writing data to Silver layer: {e}")
       raise

def transform_silver(**kwargs):
   """
   Main function to transform data from Bronze to Silver layer.
   
   This function:
   1. Sets up environment and configurations
   2. Reads data from Bronze layer
   3. Applies transformations
   4. Writes data to Silver layer with partitioning
   
   Args:
       **kwargs: Keyword arguments from Airflow
   """
   logger.info("Starting transform_silver task")
   spark = None
   try:
       # Get environment variables
       s3_bucket = os.getenv('S3_BUCKET')
       s3_bronze_path = os.getenv('S3_BRONZE_PATH')
       s3_silver_path = os.getenv('S3_SILVER_PATH')
       
       if not s3_bucket or not s3_bronze_path or not s3_silver_path:
           logger.error("S3_BUCKET, S3_BRONZE_PATH or S3_SILVER_PATH environment variables not set")
           raise ValueError("Required S3 environment variables not set")

       # Get execution date
       execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
       logger.info(f"Execution date: {execution_date}")

       # Create Spark session
       spark = create_spark_session()

       # Read Bronze data
       df_bronze = read_bronze_data(spark, s3_bucket, s3_bronze_path, execution_date)

       # Transform data
       df_silver = transform_data(df_bronze)

       # Add date column
       df_silver = df_silver.withColumn("date", lit(execution_date))

       logger.info("DataFrame schema after Silver transformations:")
       df_silver.printSchema()

       # Write to Silver layer
       write_silver_data(df_silver, spark, s3_bucket, s3_silver_path, execution_date)
       
       logger.info("transform_silver task completed successfully")
   except Exception as e:
       logger.error(f"Error in transform_silver task: {e}")
       raise
   finally:
       if spark:
           spark.stop()
           logger.info("Spark session terminated for transform_silver task")