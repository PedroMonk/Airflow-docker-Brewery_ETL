# scripts/transform_gold.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Cria e retorna uma sessão Spark"""
    try:
        logger.info("Iniciando criação da sessão Spark para transformação Gold")
        # Obter variáveis de ambiente
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

        if not aws_access_key or not aws_secret_key:
            raise ValueError("Credenciais AWS não encontradas nas variáveis de ambiente.")

        # Definir as versões compatíveis
        hadoop_aws_version = "3.3.1"        # Deve coincidir com a camada Bronze e Silver
        aws_java_sdk_version = "1.12.375"   # Deve coincidir com a camada Bronze e Silver

        # Criar sessão Spark com as dependências necessárias
        spark = SparkSession.builder \
            .appName("TransformGoldOpenBreweryDB") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},com.amazonaws:aws-java-sdk-bundle:{aws_java_sdk_version}") \
            .getOrCreate()
        
        logger.info("Sessão Spark criada com sucesso para transformação Gold")
        return spark
    except Exception as e:
        logger.error(f"Erro ao criar sessão Spark para transformação Gold: {e}")
        raise

def read_silver_data(spark, s3_bucket, s3_silver_path, execution_date):
    """Lê os dados da camada Silver particionados por 'localizacao' e 'date'"""
    try:
        logger.info(f"Lendo dados da camada Silver: s3a://{s3_bucket}/{s3_silver_path}/date={execution_date}/")
        # Lê todas as partições de 'localizacao' para a data específica
        df_silver = spark.read.option("basePath", f"s3a://{s3_bucket}/{s3_silver_path}/") \
                              .parquet(f"s3a://{s3_bucket}/{s3_silver_path}/date={execution_date}/*")
        record_count = df_silver.count()
        logger.info(f"Dados da camada Silver carregados: {record_count} registros")
        return df_silver
    except Exception as e:
        logger.error(f"Erro ao ler dados da camada Silver: {e}")
        raise

def aggregate_data(df):
    """Agrega os dados para calcular a quantidade de cervejarias por tipo e localização"""
    try:
        logger.info("Iniciando agregação dos dados para camada Gold")
        
        # Agrupar por 'brewery_type' e 'localizacao' e contar o número de cervejarias
        df_gold = df.groupBy("brewery_type", "localizacao") \
                    .agg(count("*").alias("quantity_of_breweries"))
        
        logger.info("Agregação concluída com sucesso")
        return df_gold
    except Exception as e:
        logger.error(f"Erro durante a agregação dos dados: {e}")
        raise

def write_gold_data(df, spark, s3_bucket, s3_gold_path, execution_date):
    """Escreve os dados agregados na camada Gold particionados por 'brewery_type' e 'localizacao'"""
    try:
        logger.info(f"Escrevendo dados na camada Gold: s3a://{s3_bucket}/{s3_gold_path}/date={execution_date}/")
        # Particionar por 'brewery_type' e 'localizacao'
        df.write.mode('overwrite') \
            .partitionBy("brewery_type", "localizacao") \
            .parquet(f"s3a://{s3_bucket}/{s3_gold_path}/date={execution_date}/")
        logger.info("Dados escritos com sucesso na camada Gold particionados por 'brewery_type' e 'localizacao'")
    except Exception as e:
        logger.error(f"Erro ao escrever dados na camada Gold: {e}")
        raise

def transform_gold(**kwargs):
    """Função principal para transformar dados da camada Silver para a camada Gold"""
    logger.info("Iniciando a tarefa transform_gold")
    spark = None
    try:
        # Obter variáveis de ambiente
        s3_bucket = os.getenv('S3_BUCKET')
        s3_silver_path = os.getenv('S3_SILVER_PATH')
        s3_gold_path = os.getenv('S3_GOLD_PATH')
        
        if not s3_bucket or not s3_silver_path or not s3_gold_path:
            logger.error("Variáveis S3_BUCKET, S3_SILVER_PATH ou S3_GOLD_PATH não estão definidas.")
            raise ValueError("Variáveis S3_BUCKET, S3_SILVER_PATH ou S3_GOLD_PATH não estão definidas.")

        # Recuperar a data de execução
        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
        logger.info(f"Data de execução: {execution_date}")

        # Criar sessão Spark
        spark = create_spark_session()

        # Ler dados da camada Silver
        df_silver = read_silver_data(spark, s3_bucket, s3_silver_path, execution_date)

        # Verificar se as colunas necessárias existem
        logger.info("Verificando a existência das colunas necessárias no DataFrame da camada Silver")
        expected_columns = {"brewery_type", "localizacao"}
        actual_columns = set(df_silver.columns)
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            logger.error(f"As seguintes colunas estão faltando no DataFrame da camada Silver: {missing_columns}")
            raise ValueError(f"As seguintes colunas estão faltando no DataFrame da camada Silver: {missing_columns}")
        else:
            logger.info("Todas as colunas necessárias estão presentes no DataFrame da camada Silver")

        # Agregar dados para camada Gold
        df_gold = aggregate_data(df_silver)

        # Escrever dados na camada Gold
        write_gold_data(df_gold, spark, s3_bucket, s3_gold_path, execution_date)
        
        logger.info("Tarefa transform_gold concluída com sucesso")
    except Exception as e:
        logger.error(f"Erro na tarefa transform_gold: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Sessão Spark finalizada para a tarefa transform_gold")