import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, functions as F

from src.utils import Config, get_current_date, track_execution

@track_execution(job_name="transformation")
def run_transformations(config: Config, spark: SparkSession, logger, context: dict = None):
    """Função que aplica as transformações necessárias para limpar e preparar os dados, lendo da bronze e escrevendo na silver."""
    
    expected_files = config.expected_files.values()  
    print(f"Arquivos esperados para transformação: {expected_files}")

    for table_name in expected_files:
        print(f"Processando transformação para a tabela: {table_name}")
        
        bronze_path = os.path.join(config.storage.bronze, table_name)
        silver_path = os.path.join(config.storage.silver, table_name)
        
        if os.path.exists(bronze_path):

            df = spark.read.parquet(bronze_path) \
                .filter(F.col("reference_date") == config.processing_date) \
                .transform(lambda df: _clean_invalid_values(df, config, table_name)) \
                .transform(lambda df: _format_currency(df, config, table_name)) \
                .transform(lambda df: _format_strings(df, config, table_name)) \
                .withColumn("timestamp_transform", F.lit(datetime.now().isoformat()))

            df.write.mode("overwrite").partitionBy("reference_date").parquet(silver_path)
            print(f"Transformação concluída para a tabela {table_name}. Dados salvos em: {silver_path}")
        
        else:
            raise FileNotFoundError(f"Dados da camada bronze para a tabela {table_name} e data {config.processing_date} não encontrados. Verifique se a ingestão foi realizada com sucesso.")
            
def _format_currency(df: DataFrame, config: Config, table_name: str) -> DataFrame:
    """Exemplo de transformação específica para colunas de valor monetário especificadas no YAML."""
    
    monetary_cols = config.get_cols_format(table_name, "NUM_BRL")

    if monetary_cols:
        for col in monetary_cols:
            df = df.withColumn(col, F.regexp_replace(F.col(col), r'[^\d,.-]', '')) \
                .withColumn(col, F.regexp_replace(F.col(col), r',', '.').cast('double'))   
    return df

def _clean_invalid_values(df: DataFrame, config: Config, table_name: str) -> DataFrame:
    """Exemplo de função para limpar valores inválidos configurados no YAML."""

    invalid_values = config.get_invalid_values(table_name)
    cols = config.get_cols_format(table_name,"")

    if invalid_values:
        for col in cols:
            df = df.withColumn(col, F.when(F.col(col).isin(invalid_values), None).otherwise(F.col(col)))
    return df

def _format_strings(df: DataFrame, config: Config, table_name: str) -> DataFrame:
    """Exemplo de transformação para formatar colunas de string, como remover acentos ou converter para maiúsculas."""
    
    cols_initcap = config.get_cols_format(table_name, "STR_INITC") # Todas palavras com a primeira letra maiúscula
    cols_sentcap = config.get_cols_format(table_name, "STR_SENTC") # Apenas a primeira letra da sentença em maiúscula, o restante em minúsculo

    def to_initcap(df: DataFrame) -> DataFrame:
        if cols_initcap:
            for col in cols_initcap:
                df = df.withColumn(col, F.initcap(F.col(col)))
        return df

    def to_sentcap(df: DataFrame) -> DataFrame:
        if cols_sentcap:
            for col in cols_sentcap:
                df = df.withColumn(col, 
                    F.concat(
                        F.upper(F.substring(F.col(col), 1, 1)), 
                        F.lower(F.substring(F.col(col), 2, 1000))
                        )
                    )
        return df
    
    def handle_prepositions(df: DataFrame) -> DataFrame:
        preps = ["a","o","as","os","em","de","do","da","dos","das","para",
                 "por","com","e", "sem", "sobre", "entre", "até", "desde", 
                 "contra", "à", "às", "ao", "aos", "pela", "pelo", "pelos", 
                 "pelas"]
        preps_sql = ",".join([f"'{p}'" for p in preps])
        
        for col_name in list(set(cols_sentcap + cols_initcap)):
            # F.expr permite usar expressões SQL para manipular strings, aplicando a lógica de manter preposições em minúsculo
            df = df.withColumn(col_name, F.expr(f"""
                array_join(
                    transform(
                        split(`{col_name}`, ' '),
                        x -> IF(array_contains(array({preps_sql}), lower(x)),
                                lower(x),
                                x
                        )
                    ),
                    ' '
                )
            """))
        
        return df

    df = df.transform(to_initcap) \
        .transform(to_sentcap) \
        .transform(handle_prepositions) \
    
    return df

def formate_numbers(df: DataFrame, config: Config, table_name: str) -> DataFrame:
    """Exemplo de transformação para formatar colunas numéricas, como remover caracteres não numéricos e converter para tipo numérico."""
    
    numeric_cols = config.get_cols_format(table_name, "NUM_INT")

    if numeric_cols:
        for col in numeric_cols:
            df = df.withColumn(col, F.regexp_replace(F.col(col), r'[^\d]', '').cast('integer'))
    return df
