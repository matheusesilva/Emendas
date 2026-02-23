import requests
import zipfile
import os
import shutil
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame, functions as F
from src.utils import build_spark_schema, check_schema_consistency, get_current_date, Timer, Config

def _download_zip(url: str, download_path: str, file_name: str, logger: Any) -> str:
    """Download de arquivo ZIP de uma URL para um caminho especificado."""

    # Timer para medir o tempo total do download
    download = Timer()

    logger.info(f"DOWNLOAD  | Iniciando: {url}")

    try:
        os.makedirs(download_path, exist_ok=True)
        full_path: str = os.path.join(download_path, file_name)
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(full_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        file_size: float = os.path.getsize(full_path) / (1024 * 1024)
        logger.info(f"DOWNLOAD  | Concluído: {file_name} ({file_size:.2f} MB) em {download.duration():.2f}s")
        return full_path
    
    except Exception as e:
        logger.error(f"DOWNLOAD  | Falha: {str(e)}")
        raise

def _process_to_bronze(
        zip_path: str, 
        bronze: str, 
        expected_tables: Any,
        spark: SparkSession,
        logger: Any
        ) -> None:
    
    """Processa arquivo ZIP e escreve dados em formato Bronze no caminho especificado."""

    temp_extract_path = "tmp_extraction" # Pasta temporária para extração dos arquivos do ZIP
    
    try:
        # 1. Extração
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_extract_path)
        
        # 2. Processamento baseado no Catálogo do YAML
        for table_id, table_info in vars(expected_tables).items():
            
            # Timer para medir o tempo de processamento de cada tabela
            timer = Timer()

            file_pattern = table_info.file_pattern
            full_temp_path = os.path.join(temp_extract_path, file_pattern)
            
            if not os.path.exists(full_temp_path):
                logger.warning(f"BRONZE    | Tabela {table_id} não encontrada no ZIP (esperado: {file_pattern})")
                continue

            # Construção do Schema Fixo
            spark_schema = build_spark_schema(table_info.schema)

            # Leitura do CSV com o schema definido
            df = spark.read.csv(
                full_temp_path,
                header=True,
                sep=table_info.sep,
                encoding=table_info.encoding,
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
            final_path: str = f"{bronze}/{table_id}/ingestion_date={get_current_date()}"
            df.write.mode('overwrite').parquet(final_path)
            
            logger.info(f"BRONZE    | {table_id.ljust(30)} | Sucesso | Linhas: {df.count()} | Tempo: {timer.duration():.2f}s")

    except Exception as e:
        logger.error(f"BRONZE    | Erro no processamento: {str(e)}")
        raise
    finally:
        if os.path.exists(temp_extract_path):
            shutil.rmtree(temp_extract_path)

# --- FUNÇÃO PRINCIPAL ---
def run_ingestion(config: Config, spark: SparkSession, logger: Any) -> None:
    """Função principal para rodar o processo de ingestão para uma pipeline específica."""

    # Timer para medir o tempo total do processo
    timer = Timer()
    
    logger.info(f"INGESTION | Iniciando Pipeline: {config.pipeline_name}")
    
    raw_path = os.path.join(
        config.storage.raw, 
        config.pipeline_name,
        f"download_date={get_current_date()}"
    )
    bronze_path = os.path.join(
        config.storage.bronze,
        config.pipeline_name
    )
    p_config = getattr(config.pipelines, config.pipeline_name)

    try:
        zip_path: str = _download_zip(p_config.url, raw_path, p_config.file_name, logger)
        _process_to_bronze(zip_path, bronze_path, p_config.expected_tables, spark, logger)
        
        logger.info(f"INGESTION | Finalizado com Sucesso: {config.pipeline_name} em {timer.duration():.2f}s")
    
    except Exception as e:
        logger.critical(f"INGESTION | Falha no Job: {str(e)}")