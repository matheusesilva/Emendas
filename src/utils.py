import yaml
import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Mapeador de tipos YAML -> PySpark
TYPE_MAPPER = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "double": DoubleType()
}

def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-8s | %(message)s'
        )
    return logger

def load_config(config_path="config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def build_spark_schema(yaml_schema):
    fields = []
    for col in yaml_schema:
        spark_type = TYPE_MAPPER.get(col['type'].lower(), StringType())
        fields.append(StructField(col['name'], spark_type, col.get('nullable', True)))
    return StructType(fields)

def check_schema_consistency(df, expected_schema):
    df_columns = set(df.columns)
    expected_columns = set(field.name for field in expected_schema.fields)
    
    missing_columns = expected_columns - df_columns
    new_columns = df_columns - expected_columns
    
    return missing_columns, new_columns