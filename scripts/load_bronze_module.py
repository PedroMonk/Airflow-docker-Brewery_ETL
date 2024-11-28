# scripts/load_bronze_module.py

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
# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constantes
BASE_URL = 'https://api.openbrewerydb.org/breweries'
BREWERY_TYPES = [
    'micro', 'regional', 'brewpub', 'large', 'planning', 'bar', 
    'contract', 'proprietor'
]

# Funções auxiliares
def format_request_params(page: int, per_page: int) -> Dict[str, Any]:
    return {
        'page': page,
        'per_page': per_page,
    }

def get_data(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Erro ao obter dados: {response.status_code}")
        return []

def create_spark_session():
    """Cria e retorna uma sessão Spark"""
    try:
        logger.info("Iniciando criação da sessão Spark")
        # Obter variáveis do Airflow
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_DEFAULT_REGION")

        if not aws_access_key or not aws_secret_key:
            raise ValueError("Credenciais AWS não encontradas nas variáveis do Airflow.")

        # Definir as versões compatíveis
        hadoop_aws_version = "3.3.1"        # Baseado na versão do Hadoop usada pelo Spark
        aws_java_sdk_version = "1.12.375"   # Compatível com hadoop-aws:3.3.1

        # Criar sessão Spark com as dependências necessárias
        spark = SparkSession.builder \
            .appName("LoadBronzeOpenBreweryDB") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},com.amazonaws:aws-java-sdk-bundle:{aws_java_sdk_version}") \
            .getOrCreate()
        
        logger.info("Sessão Spark criada com sucesso")
        return spark
    except Exception as e:
        logger.error(f"Erro ao criar sessão Spark: {e}")
        raise

def validate_longitude(lon: Any) -> bool:
    """Valida se a longitude pode ser convertida para float"""
    if lon is None:
        return False
    try:
        float(lon)
        return True
    except (ValueError, TypeError):
        return False

# Registrar a UDF
validate_longitude_udf = udf(validate_longitude, BooleanType())

# def extract_brewery_data(**kwargs) -> None:
#     """Função para extrair dados da API (apenas a primeira página)"""
#     logger.info("Iniciando a extração de dados da API (primeira página)")
#     data = []
#     num_breweries = 0
    
#     # Definir a página e o número de registros por página
#     page = 1
#     per_page = 50  # Ajuste conforme necessário
    
#     params = format_request_params(page=page, per_page=per_page)
#     batch = get_data(params=params)
    
#     if batch:
#         num_breweries += len(batch)
#         data.extend(batch)
#         logger.info(f"Extraídos dados de {num_breweries} cervejarias na página {page}.")
#     else:
#         logger.warning("Nenhum dado foi extraído da primeira página.")
    
#     # Passar os dados para a próxima task via XCom
#     kwargs['ti'].xcom_push(key='brewery_data', value=data)


#Função para 'n' páginas
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

def load_bronze(**kwargs) -> None:
    """Função para salvar os dados crus na camada Bronze e notificar sobre dados não padrão"""
    logger.info("Iniciando a tarefa load_bronze")
    spark = None
    try:
        # Verificar se as variáveis estão definidas
        s3_bucket = os.getenv('S3_BUCKET')
        s3_bronze_path = os.getenv('S3_BRONZE_PATH')
        
        if not s3_bucket or not s3_bronze_path:
            logger.error("Variáveis S3_BUCKET ou S3_BRONZE_PATH não estão definidas.")
            raise ValueError("Variáveis S3_BUCKET ou S3_BRONZE_PATH não estão definidas.")
        
        # Recuperar dados da task anterior via XCom
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract_brewery_data', key='brewery_data')
        
        if not data:
            logger.warning("Nenhum dado encontrado para processar")
            return  # Não levantar exceção para não falhar a DAG
        
        logger.info(f"Recebidos {len(data)} registros para carregar na camada Bronze")
    
        # Criar sessão Spark
        spark = create_spark_session()
        logger.info("Sessão Spark criada com sucesso")
        
        # Definir schema com 'longitude' e 'latitude' como StringType
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("longitude", StringType(), True),  # Alterado para StringType
            StructField("latitude", StringType(), True),   # Alterado para StringType
            StructField("phone", StringType(), True),
            StructField("website_url", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("tag_list", ArrayType(StringType()), True)
        ])
        
        logger.info("Criando DataFrame com Spark")
        # Criar DataFrame sem transformações
        df_spark = spark.createDataFrame(data, schema=schema)
        logger.info("DataFrame criado com sucesso")
        
        logger.info("Verificando dados que não atendem ao padrão")
        # Adicionar coluna para validar longitude
        df_spark = df_spark.withColumn("is_valid_longitude", validate_longitude_udf(col("longitude")))
        
        # Contar registros inválidos
        invalid_longitudes = df_spark.filter(~col("is_valid_longitude")).count()
        if invalid_longitudes > 0:
            logger.error(f"Existem {invalid_longitudes} registros com longitude inválida.")
            # Salvar registros inválidos para análise futura
            invalid_records_path = f"{s3_bronze_path}/errors/date={kwargs['execution_date'].strftime('%Y-%m-%d')}"
            df_invalid = df_spark.filter(~col("is_valid_longitude"))
            logger.info(f"Salvando registros inválidos em: s3a://{s3_bucket}/{invalid_records_path}/invalid_longitudes.json")
            df_invalid.write.mode('append').json(f"s3a://{s3_bucket}/{invalid_records_path}/invalid_longitudes.json")
        else:
            logger.info("Nenhum registro com longitude inválida encontrado.")
        
        # Remover a coluna de validação antes de salvar
        df_spark = df_spark.drop("is_valid_longitude")
        
        # Construir caminho com partição por data
        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
        s3_path = f"{s3_bronze_path}/date={execution_date}"
        logger.info(f"Salvando dados no S3 em: s3a://{s3_bucket}/{s3_path}")
        
        # Salvar no S3
        df_spark.write.mode('overwrite').parquet(f"s3a://{s3_bucket}/{s3_path}")
        logger.info(f"Dados salvos com sucesso na camada Bronze: {s3_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar dados na camada Bronze: {e}")
        raise  # Relevanta a exceção para que a task falhe e seja tentada novamente
    finally:
        if spark:
            spark.stop()
            logger.info("Sessão Spark finalizada e tarefa load_bronze concluída")


