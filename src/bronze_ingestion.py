import os
import shutil
import zipfile
import tempfile
from logging import Logger
from datetime import datetime

import requests
from pyspark.sql import SparkSession, functions as F

from src.utils import Config, get_current_date, track_execution

def extract_zip_to_tmp(zip_path: str) -> str:
    """Extrai o conteúdo do zip para uma pasta temporária e retorna o caminho."""
    print(f"Extraindo arquivo zip: {zip_path}")
    tmp_dir = tempfile.mkdtemp(prefix="emendas_tmp_")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(tmp_dir)
    return tmp_dir

@track_execution(job_name="get_zip")
def get_zip(config: Config, logger: Logger, context: dict = None) -> str:
    """Baixa o arquivo zip da URL definida no config e salva na pasta raw particionada por data."""
    
    url = config.url
    file_name = config.file_name
    save_path = os.path.join(
        config.storage.raw, 
        config.name, 
        f"download_date={config.processing_date}"
        )
    zip_path = os.path.join(save_path, file_name)

    def download_file(path: str, file: str) -> str:
        os.makedirs(path, exist_ok=True)
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            context['downloaded_file'] = file
            context['size_mb'] = round(os.path.getsize(zip_path) / (1024*1024), 2)
        else:
            raise Exception(f"Erro ao baixar arquivo: {url}")
    
    if config.processing_date == get_current_date():
        download_file(save_path, file_name)
        return zip_path

    elif os.path.exists(zip_path):
        context['downloaded_file'] = file_name
        context['size_mb'] = round(os.path.getsize(zip_path) / (1024*1024), 2)
        context['status'] = 'already_exists'
        return zip_path
    
    else:
        raise FileNotFoundError(f"Arquivo para a data {config.processing_date} não existe na camada raw.")

@track_execution(job_name="ingestion")
def run_ingestion(config: Config, spark: SparkSession, logger: Logger, zip_path: str, context: dict = None) -> None:
    """Extrai o zip, faz loop nos arquivos, verifica se são esperados, lê e salva cada CSV em parquet particionado por ingestion_date na bronze."""
    
    tmp_dir = extract_zip_to_tmp(zip_path)
    referece_date = zip_path.split("=")[1].split("/")[0]
    print(f"Data de referência extraída do nome do arquivo: {referece_date}")
    bronze_base = config.storage.bronze
    expected_files = config.expected_files

    def ingest_file(file_path: str, file_name: str) -> None:
        """Lê um arquivo CSV específico, aplica o schema, adiciona colunas de metadata e salva na camada Bronze."""     
        content_conf = getattr(config.content, file_name)
        sep = getattr(content_conf, 'separator', ';')
        encoding = getattr(content_conf, 'encoding', 'iso-8859-1')
        schema = config.build_spark_schema(file_name)
        
        df = spark.read.csv(
            file_path, 
            sep=sep, 
            encoding=encoding, 
            header=True, 
            schema=schema
            )
        df = df.withColumn("arquivo_fonte", F.lit(file_name)) \
            .withColumn("timestamp_ingestao", F.lit(datetime.now().isoformat())) \
            .withColumn("reference_date", F.lit(referece_date))
 
        source_path = os.path.join(bronze_base, file_name)
        df.write.mode("overwrite").partitionBy("reference_date").parquet(source_path)

    for file_name in os.listdir(tmp_dir):
        if file_name in expected_files:
            file_path = os.path.join(tmp_dir, file_name)
            ingest_file(file_path, expected_files[file_name])
        else:
            logger.warning(f"Arquivo inesperado ignorado: {file_name}")

    shutil.rmtree(tmp_dir)
    return 
