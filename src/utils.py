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
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-8s | %(message)s'
        )
    return logger

def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Carrega a configuração do arquivo YAML. Default é 'config.yaml' no diretório raiz."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

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