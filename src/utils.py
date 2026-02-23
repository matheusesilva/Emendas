import yaml
import logging
from typing import Dict, Set, Tuple, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Mapeador de tipos YAML -> PySpark
TYPE_MAPPER: Dict[str, Any] = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "double": DoubleType()
}

def get_logger(name: str) -> logging.Logger:
    """Configura e retorna um logger com o formato definido"""
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        log_config = load_config().get('logging', {})
        level_str = log_config.get('level', 'INFO').upper()
        level = getattr(logging, level_str, logging.INFO)
        fmt = log_config.get('format', '%(asctime)s | %(levelname)-8s | %(message)s')
        logging.basicConfig(
            level=level,
            format=fmt
        )
    return logger

def load_config(config_path: str = "config.yaml", args=None) -> Dict[str, Any]:
    """Carrega a configuração do arquivo YAML e atualiza com argumentos CLI se fornecidos."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    if args is not None:
        # Atualiza os caminhos das layers se fornecidos
        if getattr(args, 'pipeline', None):
            config['default_pipeline'] = args.pipeline
        if getattr(args, 'raw_path', None):
            config['storage']['raw'] = args.raw_path
        if getattr(args, 'bronze_path', None):
            config['storage']['bronze'] = args.bronze_path
        if getattr(args, 'silver_path', None):
            config['storage']['silver'] = args.silver_path
        if getattr(args, 'gold_path', None):
            config['storage']['gold'] = args.gold_path
        # Atualiza URL de download se fornecida
        if getattr(args, 'url', None):
            config['pipelines']['emendas_parlamentares']['url'] = args.url
        # Atualiza nível de logging se fornecido
        if getattr(args, 'log_level', None):
            config['logging']['level'] = args.log_level.upper()
    return config

def build_spark_schema(yaml_schema: List[Dict[str, Any]]) -> StructType:
    """Constrói um schema do Spark a partir da definição de schema no YAML."""
    fields: List[StructField] = []
    for col in yaml_schema:
        spark_type = TYPE_MAPPER.get(col['type'].lower(), StringType())
        fields.append(StructField(col['name'], spark_type, col.get('nullable', True)))
    return StructType(fields)

def check_schema_consistency(df: DataFrame, expected_schema: StructType) -> Tuple[Set[str], Set[str]]:
    """Verifica se as colunas do DataFrame correspondem ao schema esperado. Retorna colunas faltantes e novas colunas."""
    df_columns: Set[str] = set(df.columns)
    expected_columns: Set[str] = set(field.name for field in expected_schema.fields)
    
    missing_columns: Set[str] = expected_columns - df_columns
    new_columns: Set[str] = df_columns - expected_columns
    
    return missing_columns, new_columns