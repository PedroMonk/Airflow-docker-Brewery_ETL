"""
Bronze Layer Module - Open Brewery DB ETL Pipeline

This module handles the extraction of data from the Open Brewery DB API and loads it into 
the Bronze layer of the data lake following the medallion architecture. It includes data
extraction, basic validation, and storage in Parquet format.

The module performs the following operations:
1. Extracts brewery data from the Open Brewery DB API
2. Creates a Spark DataFrame with the raw data
3. Validates longitude values
4. Stores invalid records for future analysis
5. Saves the raw data in the Bronze layer partitioned by date

Author: [Pcosta]
Date: November 2024
"""

from airflow.models import Variable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType
import logging
import requests
from typing import Dict, Any, List
from itertools import count
import os

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = 'https://api.openbrewerydb.org/breweries'
BREWERY_TYPES = [
    'micro', 'regional', 'brewpub', 'large', 'planning', 'bar', 
    'contract', 'proprietor'
]

# Helper functions
def format_request_params(page: int, per_page: int) -> Dict[str, Any]:
    """
    Formats the request parameters for the API call.
    
    Args:
        page (int): Page number to request
        per_page (int): Number of records per page
        
    Returns:
        Dict[str, Any]: Formatted parameters dictionary
    """
    return {
        'page': page,
        'per_page': per_page,
    }

def get_data(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Retrieves data from the Open Brewery DB API.
    
    Args:
        params (Dict[str, Any]): Request parameters
        
    Returns:
        List[Dict[str, Any]]: List of brewery data dictionaries
    """
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Error fetching data: {response.status_code}")
        return []

def create_spark_session():
    """
    Creates and returns a configured Spark session for S3 access.
    
    Returns:
        SparkSession: Configured Spark session
        
    Raises:
        ValueError: If AWS credentials are not found
    """
    try:
        logger.info("Starting Spark session creation")
        # Get AWS credentials from environment variables
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_DEFAULT_REGION")

        if not aws_access_key or not aws_secret_key:
            raise ValueError("AWS credentials not found in environment variables.")

        # Define compatible versions
        hadoop_aws_version = "3.3.1"
        aws_java_sdk_version = "1.12.375"

        # Create Spark session with necessary dependencies
        spark = SparkSession.builder \
            .appName("LoadBronzeOpenBreweryDB") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},com.amazonaws:aws-java-sdk-bundle:{aws_java_sdk_version}") \
            .getOrCreate()
        
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def validate_longitude(lon: Any) -> bool:
    """
    Validates if a longitude value can be converted to float.
    
    Args:
        lon (Any): Longitude value to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    if lon is None:
        return False
    try:
        float(lon)
        return True
    except (ValueError, TypeError):
        return False

# Register UDF for longitude validation
validate_longitude_udf = udf(validate_longitude, BooleanType())

PC
siga a mesma ideia com essa função : 

def extract_brewery_data(**kwargs) -> None:
    """Função para extrair dados da API"""
    logger.info("Iniciando a extração de dados da API")
    data = []
    num_breweries = 0

    for i,page in enumerate(count(start=1)):
        params = format_request_params(page=page, per_page=50)
        batch = get_data(params=params)

        if not batch:
            logger.info(f"Parando a coleta de dados na página {page} devido a dados vazios.")
            break

        num_breweries += len(batch)
        data.extend(batch)

    # Passar os dados para a próxima task via XCom
    kwargs['ti'].xcom_push(key='brewery_data', value=data)
    logger.info(f"Extraídos dados de {num_breweries} cervejarias.")


Here's the documented English version of the extract_brewery_data function for multiple pages:

python

Copy
def extract_brewery_data(**kwargs) -> None:
    """
    Extracts brewery data from the Open Brewery DB API, iterating through all available pages.

    This function performs the following operations:
    1. Iterates through API pages until no more data is available
    2. Collects brewery data from each page
    3. Stores the accumulated data in Airflow's XCom for the next task

    Args:
        **kwargs: Keyword arguments from Airflow, containing the task instance (ti)
                 needed for XCom communication
    
    Returns:
        None: The extracted data is pushed to XCom with key 'brewery_data'
    """
    logger.info("Starting API data extraction")
    data = []
    num_breweries = 0
    
    for i, page in enumerate(count(start=1)):
        params = format_request_params(page=page, per_page=50)
        batch = get_data(params=params)
        
        if not batch:
            logger.info(f"Stopping data collection at page {page} due to empty response")
            break
            
        num_breweries += len(batch)
        data.extend(batch)
    
    # Pass the extracted data to the next task via XCom
    kwargs['ti'].xcom_push(key='brewery_data', value=data)
    logger.info(f"Successfully extracted data from {num_breweries} breweries")

def load_bronze(**kwargs) -> None:
    """
    Loads raw data into the Bronze layer and handles data validation.
    
    Performs the following steps:
    1. Retrieves data from XCom
    2. Creates a Spark DataFrame
    3. Validates longitude values
    4. Stores invalid records separately
    5. Saves valid data in Parquet format
    """
    logger.info("Starting load_bronze task")
    spark = None
    try:
        # Check environment variables
        s3_bucket = os.getenv('S3_BUCKET')
        s3_bronze_path = os.getenv('S3_BRONZE_PATH')
        
        if not s3_bucket or not s3_bronze_path:
            logger.error("S3_BUCKET or S3_BRONZE_PATH environment variables not set")
            raise ValueError("Required S3 environment variables not set")
        
        # Retrieve data from previous task
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract_brewery_data', key='brewery_data')
        
        if not data:
            logger.warning("No data found to process")
            return
        
        logger.info(f"Received {len(data)} records to load into Bronze layer")
    
        # Create Spark session
        spark = create_spark_session()
        logger.info("Spark session created successfully")
        
        # Define schema
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("website_url", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("tag_list", ArrayType(StringType()), True)
        ])
        
        logger.info("Creating Spark DataFrame")
        df_spark = spark.createDataFrame(data, schema=schema)
        logger.info("DataFrame created successfully")
        
        logger.info("Checking for non-standard data")
        # Add validation column for longitude
        df_spark = df_spark.withColumn("is_valid_longitude", validate_longitude_udf(col("longitude")))
        
        # Count invalid records
        invalid_longitudes = df_spark.filter(~col("is_valid_longitude")).count()
        if invalid_longitudes > 0:
            logger.error(f"Found {invalid_longitudes} records with invalid longitude.")
            # Save invalid records for future analysis
            invalid_records_path = f"{s3_bronze_path}/errors/date={kwargs['execution_date'].strftime('%Y-%m-%d')}"
            df_invalid = df_spark.filter(~col("is_valid_longitude"))
            logger.info(f"Saving invalid records to: s3a://{s3_bucket}/{invalid_records_path}/invalid_longitudes.json")
            df_invalid.write.mode('append').json(f"s3a://{s3_bucket}/{invalid_records_path}/invalid_longitudes.json")
        else:
            logger.info("No invalid longitude records found.")
        
        # Remove validation column before saving
        df_spark = df_spark.drop("is_valid_longitude")
        
        # Build path with date partition
        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
        s3_path = f"{s3_bronze_path}/date={execution_date}"
        logger.info(f"Saving data to S3: s3a://{s3_bucket}/{s3_path}")
        
        # Save to S3
        df_spark.write.mode('overwrite').parquet(f"s3a://{s3_bucket}/{s3_path}")
        logger.info(f"Data successfully saved to Bronze layer: {s3_path}")
    except Exception as e:
        logger.error(f"Error saving data to Bronze layer: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session terminated and load_bronze task completed")
