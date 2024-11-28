# scripts/transform_silver.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, lit
from pyspark.sql.types import FloatType
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Cria e retorna uma sessão Spark"""
    try:
        logger.info("Iniciando criação da sessão Spark para transformação Silver")
        # Obter variáveis de ambiente
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

        if not aws_access_key or not aws_secret_key:
            raise ValueError("Credenciais AWS não encontradas nas variáveis de ambiente.")

        # Definir as versões compatíveis
        hadoop_aws_version = "3.3.1"        # Deve coincidir com a camada Bronze
        aws_java_sdk_version = "1.12.375"   # Deve coincidir com a camada Bronze

        # Criar sessão Spark com as dependências necessárias
        spark = SparkSession.builder \
            .appName("TransformSilverOpenBreweryDB") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},com.amazonaws:aws-java-sdk-bundle:{aws_java_sdk_version}") \
            .getOrCreate()
        
        logger.info("Sessão Spark criada com sucesso para transformação Silver")
        return spark
    except Exception as e:
        logger.error(f"Erro ao criar sessão Spark para transformação Silver: {e}")
        raise

def read_bronze_data(spark, s3_bucket, s3_bronze_path, execution_date):
    """Lê os dados da camada Bronze"""
    try:
        logger.info(f"Lendo dados da camada Bronze: s3a://{s3_bucket}/{s3_bronze_path}/date={execution_date}")
        df_bronze = spark.read.parquet(f"s3a://{s3_bucket}/{s3_bronze_path}/date={execution_date}")
        record_count = df_bronze.count()
        logger.info(f"Dados da camada Bronze carregados: {record_count} registros")
        return df_bronze
    except Exception as e:
        logger.error(f"Erro ao ler dados da camada Bronze: {e}")
        raise

def transform_data(df):
    """Realiza transformações nos dados"""
    try:
        logger.info("Iniciando transformações na camada Silver")

        # Remover duplicatas com base no campo 'id'
        df = df.dropDuplicates(["id"])
        record_count = df.count()
        logger.info(f"Duplicatas removidas. Total de registros: {record_count}")

        # Tratar valores nulos
        # Exemplo: Substituir valores nulos em 'phone' por 'N/A'
        df = df.withColumn("phone", when(col("phone").isNull(), "N/A").otherwise(col("phone")))
        # Adicione mais tratamentos conforme necessário

        # Converter tipos de dados
        # Exemplo: Converter 'longitude' e 'latitude' para FloatType
        df = df.withColumn("longitude", col("longitude").cast(FloatType()))
        df = df.withColumn("latitude", col("latitude").cast(FloatType()))

        # Adicionar coluna 'localizacao' se ainda não existir
        # Supondo que 'city' e 'state' existam
        if "localizacao" not in df.columns:
            df = df.withColumn("localizacao", concat_ws(", ", col("city"), col("state")))
            logger.info("Coluna 'localizacao' adicionada ao DataFrame")
        else:
            logger.info("Coluna 'localizacao' já existe no DataFrame")

        logger.info("Tipos de dados convertidos com sucesso e coluna 'localizacao' garantida")

        logger.info("Transformações na camada Silver concluídas")
        return df
    except Exception as e:
        logger.error(f"Erro durante as transformações na camada Silver: {e}")
        raise

def write_silver_data(df, spark, s3_bucket, s3_silver_path, execution_date):
    """Escreve os dados transformados na camada Silver particionados por 'localizacao' e 'date'"""
    try:
        logger.info(f"Escrevendo dados na camada Silver: s3a://{s3_bucket}/{s3_silver_path}/date={execution_date}")
        # Adicionar coluna 'date' como literal
        df = df.withColumn("date", lit(execution_date))

        # Particionar por 'localizacao' e 'date'
        df.write.mode('overwrite') \
            .partitionBy("localizacao", "date") \
            .parquet(f"s3a://{s3_bucket}/{s3_silver_path}/date={execution_date}")
        logger.info("Dados escritos com sucesso na camada Silver particionados por 'localizacao' e 'date'")
    except Exception as e:
        logger.error(f"Erro ao escrever dados na camada Silver: {e}")
        raise

def transform_silver(**kwargs):
    """Função principal para transformar dados da camada Bronze para a camada Silver"""
    logger.info("Iniciando a tarefa transform_silver")
    spark = None
    try:
        # Obter variáveis de ambiente
        s3_bucket = os.getenv('S3_BUCKET')
        s3_bronze_path = os.getenv('S3_BRONZE_PATH')
        s3_silver_path = os.getenv('S3_SILVER_PATH')
        
        if not s3_bucket or not s3_bronze_path or not s3_silver_path:
            logger.error("Variáveis S3_BUCKET, S3_BRONZE_PATH ou S3_SILVER_PATH não estão definidas.")
            raise ValueError("Variáveis S3_BUCKET, S3_BRONZE_PATH ou S3_SILVER_PATH não estão definidas.")

        # Recuperar a data de execução
        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
        logger.info(f"Data de execução: {execution_date}")

        # Criar sessão Spark
        spark = create_spark_session()

        # Ler dados da camada Bronze
        df_bronze = read_bronze_data(spark, s3_bucket, s3_bronze_path, execution_date)

        # Transformar dados
        df_silver = transform_data(df_bronze)

        # Adicionar coluna 'date' com o valor de execution_date
        df_silver = df_silver.withColumn("date", lit(execution_date))  # Linha corrigida

        logger.info("Schema do DataFrame após transformações na camada Silver:")
        df_silver.printSchema()

        # Escrever dados na camada Silver particionados por 'localizacao' e 'date'
        write_silver_data(df_silver, spark, s3_bucket, s3_silver_path, execution_date)
        
        logger.info("Tarefa transform_silver concluída com sucesso")
    except Exception as e:
        logger.error(f"Erro na tarefa transform_silver: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Sessão Spark finalizada para a tarefa transform_silver")