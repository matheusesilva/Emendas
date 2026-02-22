import requests
import zipfile
import os
import shutil
import time
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from src.utils import get_logger, build_spark_schema, check_schema_consistency

# --- CONFIGURAÇÃO DE LOGGING ---
logger = get_logger("IngestionEngine")

def _download_zip(url: str, download_path: str, file_name: str) -> str:
    """Download de arquivo ZIP de uma URL para um caminho especificado."""
    full_path: str = os.path.join(download_path, file_name)
    os.makedirs(download_path, exist_ok=True)
    
    logger.info(f"DOWNLOAD  | Iniciando: {url}")
    start_time: float = time.time()
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(full_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        file_size: float = os.path.getsize(full_path) / (1024 * 1024)
        logger.info(f"DOWNLOAD  | Concluído: {file_name} ({file_size:.2f} MB) em {time.time() - start_time:.2f}s")
        return full_path
    except Exception as e:
        logger.error(f"DOWNLOAD  | Falha: {str(e)}")
        raise

def _process_to_bronze(
        zip_path: str, 
        bronze: str, 
        expected_tables: Dict[str, Any],
        spark: SparkSession,
        ) -> None:
    
    """Processa arquivo ZIP e escreve dados em formato Bronze no caminho especificado."""

    spark.sparkContext.setLogLevel("ERROR")
    temp_extract_path: str = "tmp_extraction"
    current_date: str = datetime.now().strftime('%Y-%m-%d')
    
    try:
        # 1. Extração
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_extract_path)
        
        # 2. Processamento baseado no Catálogo do YAML
        for table_id, table_info in expected_tables.items():
            start_table: float = time.time()
            file_pattern: str = table_info['file_pattern']
            full_temp_path: str = os.path.join(temp_extract_path, file_pattern)
            
            if not os.path.exists(full_temp_path):
                logger.warning(f"BRONZE    | Tabela {table_id} não encontrada no ZIP (esperado: {file_pattern})")
                continue

            # Construção do Schema Fixo
            spark_schema: StructType = build_spark_schema(table_info['schema'])

            # Leitura do CSV com o schema definido
            df: DataFrame = spark.read.csv(
                full_temp_path,
                header=True,
                sep=table_info.get('sep', ';'),
                encoding=table_info.get('encoding', 'iso-8859-1'),
                schema=spark_schema
            )

            # Verificação de Colunas Novas
            missing_columns, new_columns = check_schema_consistency(df, spark_schema)
            if missing_columns:
                logger.warning(f"BRONZE    | {table_id} | Esquema inconsistente: Colunas faltando - {missing_columns}")
            if new_columns: 
                logger.warning(f"BRONZE    | {table_id} | Esquema inconsistente: Colunas novas - {new_columns}")
            
            # Auditoria
            df = df.withColumn("ingested_at", F.current_timestamp()) \
                   .withColumn("source_file", F.lit(file_pattern))
            
            # Escrita Parquet Particionada
            # Estrutura: data/01_bronze/nome_tabela/ingestion_date=YYYY-MM-DD/
            final_path: str = f"{bronze}/{table_id}/ingestion_date={current_date}"
            df.write.mode('overwrite').parquet(final_path)
            
            duration: float = time.time() - start_table
            logger.info(f"BRONZE    | {table_id.ljust(30)} | Sucesso | Linhas: {df.count()} | Tempo: {duration:.2f}s")

    except Exception as e:
        logger.error(f"BRONZE    | Erro no processamento: {str(e)}")
        raise
    finally:
        if os.path.exists(temp_extract_path):
            shutil.rmtree(temp_extract_path)

# --- FUNÇÃO PRINCIPAL ---
def run_ingestion(pipeline_name: str, config: Any, spark: SparkSession) -> None:

    """Função principal para rodar o processo de ingestão para uma pipeline específica."""
    
    p_config: Dict[str, Any] | None = config['pipelines'].get(pipeline_name)
    storage: Dict[str, Any] = config['storage']

    logger.info(f"INGESTION | Iniciando Pipeline: {pipeline_name}")
    total_start: float = time.time()
    
    # Caminhos de armazenamento
    raw_path: str = os.path.join(
        storage['raw'], 
        pipeline_name,
        f"transform_date={datetime.now().strftime('%Y-%m-%d')}"
    )

    bronze_path: str = os.path.join(
        storage['bronze'],
        pipeline_name
    )

    try:
        # Execução das etapas
        zip_path: str = _download_zip(p_config['url'], raw_path, p_config['file_name'])
        _process_to_bronze(zip_path, bronze_path, p_config['expected_tables'], spark)

        total_duration: float = time.time() - total_start
        logger.info(f"INGESTION | Finalizado com Sucesso: {pipeline_name} em {total_duration:.2f}s")
    except Exception as e:
        logger.critical(f"INGESTION | Falha no Job: {str(e)}")