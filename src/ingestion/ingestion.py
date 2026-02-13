import requests
import logging
import zipfile
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


DEFAULT_URL = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/emendas-parlamentares/EmendasParlamentares.zip"
ORIGINAL_DATA_PATH = f"data/raw/original/emendas_parlamentares_{datetime.now().strftime('%Y%m%d')}.zip"
EXTRACTED_DATA_PATH = "data/raw/extracted/"
BRONZE_DATA_PATH = "data/bronze/emendas_parlamentares/"

logger = logging.getLogger(__name__)

def _download_zip(url, download_path):
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an error for HTTP errors
        with open(download_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"Successfully downloaded zip file from {url} to {download_path}")
    except requests.RequestException as e:
        logger.error(f"Error downloading zip file: {e}")
        raise

def _extract_zip(dowload_path, extracted_data_path):
    # Ensure the extraction directory exists
    daily_ingestion_folder = os.path.join(extracted_data_path , datetime.now().strftime('%Y%m%d'))
    os.makedirs(daily_ingestion_folder, exist_ok=True)
    try:
        with zipfile.ZipFile(dowload_path, 'r') as zip_ref:
            zip_ref.extractall(daily_ingestion_folder)
        logger.info(f"Successfully extracted zip file from {dowload_path} to {daily_ingestion_folder}")
    except zipfile.BadZipFile as e:
        logger.error(f"Error extracting zip file: {e}")
        raise

def _save_to_bronze(extracted_path, bronze_path):
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Emendas") \
        .getOrCreate()
    
    # Define schema for the CSV file
    emendas_schema = StructType([
        StructField("codigo_da_emenda", StringType(), True),
        StructField("ano_da_emenda", IntegerType(), True),
        StructField("tipo_de_emenda", StringType(), True),
        StructField("codigo_do_autor_da_emenda", StringType(), True),
        StructField("nome_do_autor_da_emenda", StringType(), True),
        StructField("numero_da_emenda", StringType(), True),
        StructField("localidade_de_aplicacao_do_recurso", StringType(), True),
        StructField("codigo_municipio_ibge", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("codigo_uf_ibge", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("regiao", StringType(), True),
        StructField("codigo_funcao", StringType(), True),
        StructField("nome_funcao", StringType(), True),
        StructField("codigo_subfuncao", StringType(), True),
        StructField("nome_subfuncao", StringType(), True),
        StructField("codigo_programa", StringType(), True),
        StructField("nome_programa", StringType(), True),
        StructField("codigo_acao", StringType(), True),
        StructField("nome_acao", StringType(), True),
        StructField("codigo_plano_orcamentario", StringType(), True),
        StructField("nome_plano_orcamentario", StringType(), True),
        StructField("valor_empenhado", DoubleType(), True),
        StructField("valor_liquidado", DoubleType(), True),
        StructField("valor_pago", DoubleType(), True),
        StructField("valor_restos_a_pagar_inscritos", DoubleType(), True),
        StructField("valor_restos_a_pagar_cancelados", DoubleType(), True),
        StructField("valor_restos_a_pagar_pagos", DoubleType(), True),
    ])

    try:
        df = spark.read \
            .format("csv") \
            .option("delimiter", ";") \
            .option("header", "true") \
            .option("schema", emendas_schema) \
            .load(f"{extracted_path}EmendasParlamentares.csv")
        
        df.write.mode('overwrite').parquet(bronze_path)
        logger.info(f"Successfully saved DataFrame to {bronze_path}")
    except Exception as e:
        logger.error(f"Error saving DataFrame to Parquet: {e}")
        raise

def ingest_data(
        url=DEFAULT_URL,
        download_path=ORIGINAL_DATA_PATH,
        extracted_path=EXTRACTED_DATA_PATH,
        bronze_path=BRONZE_DATA_PATH
        ):
    _download_zip(url, download_path)
    _extract_zip(download_path, extracted_path)
    _save_to_bronze(extracted_path, bronze_path)
    return True