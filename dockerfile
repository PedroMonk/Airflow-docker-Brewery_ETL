# Use a imagem oficial do Apache Airflow com Python 3.9
FROM apache/airflow:2.10.3-python3.9

# Trocar para root para instalar pacotes do sistema
USER root

# Instalar Java 17 e utilitários do sistema
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    wget \
    curl \
    procps \
    sudo \
    && rm -rf /var/lib/apt/lists/* \
    && echo "airflow ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# Definir JAVA_HOME e SPARK_HOME, e adicionar ao PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark-3.4.1-bin-hadoop3
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

# Baixar e instalar Spark 3.4.1
RUN wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz \
    && tar -xzf spark-3.4.1-bin-hadoop3.tgz -C /opt/ \
    && rm spark-3.4.1-bin-hadoop3.tgz

# Criar o grupo airflow, se não existir
RUN getent group airflow || groupadd -r airflow

# Configurar diretórios e permissões
RUN mkdir -p /opt/airflow/logs /opt/airflow/scripts /opt/airflow/dags /opt/airflow/plugins \
    && mkdir -p /home/airflow/.local /home/airflow/.cache/pip \
    && chown -R airflow:airflow /opt/airflow /home/airflow \
    && chmod -R 755 /opt/airflow /home/airflow

# Definir a variável de ambiente HOME para o usuário airflow
ENV HOME=/home/airflow
ENV PYTHONPATH=/opt/airflow/scripts:/opt/airflow/dags

# Copiar arquivos com propriedade correta
COPY --chown=airflow:airflow requirements.txt /opt/airflow/requirements.txt
COPY --chown=airflow:airflow scripts/ /opt/airflow/scripts/
COPY --chown=airflow:airflow dags/ /opt/airflow/dags/
COPY --chown=airflow:airflow plugins/ /opt/airflow/plugins/

# Mudar para o usuário airflow para executar pip
USER airflow

# Atualizar pip e instalar dependências
RUN pip install --upgrade pip \
    && pip install --no-cache-dir \
        pyspark==3.4.1 \
        delta-spark==2.4.0 \
        apache-airflow-providers-apache-spark==4.1.5

# Definir variáveis de ambiente adicionais se necessário
ENV PYTHONUNBUFFERED=1