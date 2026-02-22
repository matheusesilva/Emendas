import re
from typing import Any
import unicodedata
import time
import os
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.utils import get_logger

logger = get_logger("SilverTransformation")

# Agrupar colunas por tipo de transformação para evitar múltiplas passagens no DataFrame


def _clean_column_names(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Limpa nomes de colunas: remove acentos, caracteres especiais, 
    converte para snake_case e letras minúsculas.
    """
    def transform_name(name):
        # Normaliza para remover acentos (NFD separa o caractere do acento)
        name = unicodedata.normalize('NFD', name)
        name = name.encode('ascii', 'ignore').decode('utf-8')
        
        # Converte para minúsculo
        name = name.lower()
        
        # Substitui espaços, hífens e pontos por underscore
        name = re.sub(r'[\s\-\.]+', '_', name)
        
        # Remove qualquer caractere que não seja letra (a-z), número ou underscore
        name = re.sub(r'[^a-z0-9_]', '', name)
        
        # Limpeza de underscores extras (duplicados ou nas extremidades)
        name = re.sub(r'_+', '_', name)
        return name.strip('_')

    # Renomeando todas as colunas de uma vez para melhor performance
    new_columns = [transform_name(c) for c in df.columns]
    return df.toDF(*new_columns)

def _clean_monetary_values(df: DataFrame, cols_to_clean: list, spark: SparkSession) -> DataFrame:
    """
    Limpa e padroniza colunas com valores monetários, removendo símbolos de moeda, 
    pontos de milhar e convertendo vírgulas decimais para pontos.
    Ex: "R$ 1.234,56" -> "1234.56"
    """
    for col_name in cols_to_clean:
        logger.info(f"TRANSFORM | Limpando coluna monetária: {col_name}")
        c = F.col(col_name)
        c = F.regexp_replace(c, r'[R\$\s]', '') # Remove R, $ e espaços
        c = F.regexp_replace(c, r'\.', '')      # Remove pontos de milhar
        c = F.regexp_replace(c, r',', '.')      # Vírgula para ponto
        
    df = df.withColumn(col_name, c.cast('double'))
    return df

def _capitalize_first(df: DataFrame, cols_to_transform: list, spark: SparkSession) -> DataFrame:
    """
    Aplica Maiúscula apenas no primeiro caractere da string em várias colunas.
    Ex: "RELATÓRIO DE VENDAS" -> "Relatório de vendas"
    """
    for col_name in cols_to_transform:
        col = F.col(col_name)
        # 1ª letra em Upper + Restante da string em Lower
        transformed = F.concat(
            F.upper(F.substring(col, 1, 1)),
            F.lower(F.substring(col, 2, 100000))
        )
        df = df.withColumn(col_name, transformed)
    return df

def _capitalize_all(df: DataFrame, cols_to_transform: list, spark: SparkSession) -> DataFrame:
    """
    Aplica Initcap em várias colunas.
    Ex: "JOÃO SILVA" -> "João Silva"
    """
    for col_name in cols_to_transform:
        df = df.withColumn(col_name, F.initcap(F.col(col_name)))
    return df

def _clean_invalues(df: DataFrame, cols_to_clean: list, invalid_values: list, spark: SparkSession) -> DataFrame:
    """
    Limpa valores indesejados em colunas específicas, como "null", "n/a", "desconecido", etc.
    Substitui por null real do Spark.
    """
    invalid_values = [v.lower() for v in invalid_values]  # Padroniza para comparação case-insensitive

    for col_name in cols_to_clean:
        df = df.withColumn(
            col_name,
            F.when(F.lower(F.col(col_name)).isin(invalid_values), None).otherwise(F.col(col_name))
        )
    return df

def run_transformations(pipeline_name: str, config: Any, spark: SparkSession) -> None:

    """Função principal para aplicar todas as transformações de limpeza e padronização."""

    p_config = config['pipelines'].get(pipeline_name)
    storage = config['storage']

    logger.info(f"TRANSFORM | Iniciando Transformações: {pipeline_name}")
    total_start: float = time.time()

    # Carrega tabelas da camada Bronze
    expected_tables = p_config['expected_tables']
    for table_name, table_config in expected_tables.items():
        start_table: float = time.time()
        bronze_path = os.path.join(storage['bronze'], pipeline_name, table_name)
        df = spark.read.parquet(bronze_path)

        # 1. Limpeza de Valores Indesejados
        invalid_values = p_config.get('invalid_values', [])
        if invalid_values:
            logger.info(f"TRANSFORM | Limpando valores inválidos: {invalid_values} da tabela {table_name}")
            df = _clean_invalues(df, df.columns, invalid_values, spark)

        # 2. Limpeza e formatação colunas com valores monetários
        monetary_cols = [col['name'] for col in table_config['schema'] if col['format'].startswith('NUM_BRL')]
        print(monetary_cols)
        if monetary_cols:
            logger.info(f"TRANSFORM | Limpando colunas monetárias: {monetary_cols} da tabela {table_name}")
            df =  _clean_monetary_values(df, monetary_cols, spark)
    
        # Salva o DataFrame transformado na camada Silver
        silver_path = os.path.join(
            storage['silver'], 
            pipeline_name, 
            table_name, 
            f"download_date={datetime.now().strftime('%Y-%m-%d')}" 
            )
        df.write.mode("overwrite").parquet(silver_path)
        logger.info(f"TRANSFORM | Tabela {table_name} salva na camada Silver em: {silver_path}")

        end_table: float = time.time()
        logger.info(f"TRANSFORM | Tabela {table_name} transformada em {end_table - start_table:.2f} segundos")