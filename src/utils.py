import yaml
import logging
import time
import json
from datetime import datetime
from types import SimpleNamespace
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

DEFAULT_YAML_CONFIG: str = "config.yaml"

def build_spark_schema(yaml_schema: List[Dict[str, Any]]) -> StructType:
    """Constrói um schema do Spark a partir da definição de schema no YAML."""
    fields: List[StructField] = []
    for col in yaml_schema:
        spark_type = TYPE_MAPPER.get(col.type.lower(), StringType())
        fields.append(StructField(col.name, spark_type))
    return StructType(fields)

def check_schema_consistency(df: DataFrame, expected_schema: StructType) -> Tuple[Set[str], Set[str]]:
    """Verifica se as colunas do DataFrame correspondem ao schema esperado. Retorna colunas faltantes e novas colunas."""
    df_columns = set(df.columns)
    expected_columns = set(field.name for field in expected_schema.fields)
    
    missing_columns = expected_columns - df_columns
    new_columns = df_columns - expected_columns
    
    return missing_columns, new_columns

def create_logger(config: Config) -> logging.Logger:
    """Configura e retorna um logger com o formato definido"""
    logger = logging.getLogger(config.pipeline_name)
    if not logger.hasHandlers():
        level = getattr(logging, config.logging.level)
        format = config.logging.format
        logging.basicConfig(
            level=level,
            format=format
        )
    return logger

def get_current_date() -> str:
    """Retorna a data atual no formato YYYY-MM-DD"""
    return datetime.now().strftime('%Y-%m-%d')

class Timer:
    """Context manager para medir o tempo de execução de blocos de código."""
    def __init__(self):
        self.__start_time: float = 0.0
        self.__end_time: float = 0.0
        self.start()

    def start (self):
        self.__start_time = time.time()
    
    def duration(self):
        self.__end_time = time.time()
        return self.__end_time - self.__start_time 
    
class Config: 
    """Classe para representar a configuração do pipeline, carregada do YAML."""
    def __init__(self, args=None):
        self._args = args
        self._config_file = self._get_file()
        self._config = self._read_config()
        self._replace_with_args()
        self._as_obj = self._to_obj()

    def __getattr__(self, name):
        return getattr(self._as_obj, name)
    
    @property
    def config(self):
        return self._config
    
    @property
    def file(self):
        return self._config_file
    
    @property
    def pipeline_name(self):
        return self._config.get('default_pipeline')

    def _to_obj(self) -> Any:
        #Converte um dicionário aninhado em um objeto de namespace para acesso via atributo
        return json.loads(json.dumps(self._config), object_hook=lambda x: SimpleNamespace(**x))
    
    def _get_file(self) -> str:
        if self._args is not None and self._args.config:
            return self._args.config
        return DEFAULT_YAML_CONFIG
    
    def _read_config(self) -> Dict[str, Any]:
        try:
            with open(self._config_file, "r") as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            logging.error(f"CONFIG    | Falha ao carregar {self._config_file}: {str(e)}")
            raise Exception(f"Falha ao carregar configuração: {str(e)}")
    
    def _replace_with_args(self) -> Dict[str, Any]:
        # Sobrescreve configurações com argumentos CLI, se fornecidos
        if self._args is not None:
            if self._args.pipeline: self._config['default_pipeline'] = self._args.pipeline
            if self._args.raw_path: self._config['storage']['raw'] = self._args.raw_path
            if self._args.bronze_path: self._config['storage']['bronze'] = self._args.bronze_path
            if self._args.silver_path: self._config['storage']['silver'] = self._args.silver_path
            if self._args.gold_path: self._config['storage']['gold'] = self._args.gold_path
            if self._args.url: self._config['pipelines']['emendas_parlamentares']['url'] = self._args.url
            if self._args.log_level: self._config['logging']['level'] = self._args.log_level.upper()