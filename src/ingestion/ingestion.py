import yaml
import requests
import logging
import zipfile
import os
import shutil
import time
from datetime import datetime
from pyspark.sql import SparkSession, functions as F

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)
logger = logging.getLogger("IngestionEngine")

# --- FUNÇÕES AUXILIARES ---
def load_config(config_path="config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def _download_zip(url, download_path, file_name):
    full_path = os.path.join(download_path, file_name)
    os.makedirs(download_path, exist_ok=True)
    
    logger.info(f"DOWNLOAD | Iniciando: {url}")
    start_time = time.time()
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(full_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        file_size = os.path.getsize(full_path) / (1024 * 1024)  # Convertendo para MB
        logger.info(f"DOWNLOAD | Concluído: {file_name} ({file_size:.2f} MB) em {time.time() - start_time:.2f} segundos")
        return full_path
    except Exception as e:
        logger.error(f"DOWNLOAD | Falha: {str(e)}")
        raise

def _process_to_bronze(zip_path, bronze_base_path, options):
    spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    temp_extract_path = "tmp"
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_extract_path)
            files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
        
        for file_name in files:
            table_name = file_name.replace('.csv', '').lower()
            full_temp_path = os.path.join(temp_extract_path, file_name)
            
            # Usando as opções vindas do YAML
            df = spark.read.csv(
                full_temp_path, 
                header=True, 
                sep=options.get('sep', ','), 
                encoding=options.get('encoding', 'utf-8'),
                inferSchema=options.get('inferSchema', True)
            )
            
            df = df.withColumn("ingested_at", F.current_timestamp()) \
                   .withColumn("source_file", F.lit(file_name))
            
            # Caminho: bronze/tabela/partição
            final_path = f"{bronze_base_path}/{table_name}/ingestion_date={current_date}"
            df.write.mode('overwrite').parquet(final_path)
            
            logger.info(f"BRONZE | {table_name} | Salvo com {df.count()} registros.")

    finally:
        if os.path.exists(temp_extract_path):
            shutil.rmtree(temp_extract_path)

# --- PIPELINE PRINCIPAL ---
def run_ingestion(pipeline_name):
    config = load_config()
    
    # Extrai configs específicas do pipeline solicitado
    p_config = config['pipelines'].get(pipeline_name)
    storage = config['storage']
    
    if not p_config:
        logger.error(f"Configuração para '{pipeline_name}' não encontrada no YAML.")
        return

    logger.info(f"INGESTION | Iniciando Job: {pipeline_name}")
    start_time = time.time()
    
    raw_path = os.path.join(
        storage['raw_base_path'], 
        pipeline_name,
        f"download_date={datetime.now().strftime('%Y-%m-%d')}"
        )
    
    bronze_path = os.path.join(
        storage['bronze_base_path'], 
        pipeline_name
        )

    zip_path = _download_zip(p_config['url'], raw_path, p_config['file_name'])
    _process_to_bronze(zip_path, bronze_path, p_config['options'])

    logger.info(f"INGESTION | Concluído Job: {pipeline_name} em {time.time() - start_time:.2f} segundos")

if __name__ == "__main__":
    run_ingestion('emendas_parlamentares')