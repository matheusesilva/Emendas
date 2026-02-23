import yaml
import logging
import time
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
        level = getattr(logging, level_str)
        fmt = log_config.get('format')
        logging.basicConfig(
            level=level,
            format=fmt
        )
    return logger

def load_config(args=None) -> Dict[str, Any]:
    """Carrega a configuração do arquivo YAML e atualiza com argumentos CLI se fornecidos."""
    try:
        config_path = args.config # Default é 'config.yaml' se não for fornecido via CLI
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logging.error(f"CONFIG    | Falha ao carregar {config_path}: {str(e)}")
        raise Exception(f"Falha ao carregar configuração: {str(e)}")
    
    # Sobrescreve configurações com argumentos CLI, se fornecidos
    if args is not None:
        if args.pipeline: config['default_pipeline'] = args.pipeline
        if args.raw_path: config['storage']['raw'] = args.raw_path
        if args.bronze_path: config['storage']['bronze'] = args.bronze_path
        if args.silver_path: config['storage']['silver'] = args.silver_path
        if args.gold_path: config['storage']['gold'] = args.gold_path
        if args.url: config['pipelines']['emendas_parlamentares']['url'] = args.url
        if args.log_level: config['logging']['level'] = args.log_level.upper()
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

class Timer:
    """Context manager para medir o tempo de execução de blocos de código."""
    def __init__(self):
        self.start_time: float = 0.0
        self.end_time: float = 0.0

    def start (self):
        self.start_time = time.time()
    
    def duration(self):
        self.end_time = time.time()
        return self.end_time - self.start_time