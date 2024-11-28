from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

s3_bucket = os.getenv('S3_BUCKET')
s3_bronze_path= os.getenv('S3_BRONZE_PATH')
# Crie uma sessão Spark
spark = SparkSession.builder \
    .appName("VisualizarCamadaBronze") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Defina o caminho para a camada Bronze
s3_bucket = "breweris-etl-prod-sp"
execution_date = "2024-11-28"  # Substitua pela data desejada
s3_bronze_path = f"s3a://{s3_bucket}/bronze/date={execution_date}/"

# Leia os dados da camada Bronze
df_bronze = spark.read.parquet(s3_bronze_path)

# Mostre as primeiras linhas
df_bronze.show(20, truncate=False)

# (Opcional) Imprima o esquema do DataFrame
df_bronze.printSchema()

# Pare a sessão Spark quando terminar
spark.stop()