from pyspark.sql import SparkSession
from src.bronze_ingestion import run_ingestion
from src.silver_transformation import run_transformations
# from gold_delivery import run_delivery # Ainda não implementado
from src.utils import get_logger, load_config

class ETLPipeline:
    def __init__(self, args=None):
        self.args = args
        self.name = None
        self.config = None
        self.logger = None
        self.spark_session = None

    def _set_config(self):
        # Usa config via CLI se fornecida, senão carrega do arquivo YAML
        self.config = load_config(args=self.args)
        self.name = self.config.get('default_pipeline')

    def _set_logger(self):
        self.logger = get_logger(self.name)

    def _create_session(self):
        self.spark_session = SparkSession.builder.appName(self.name).getOrCreate()

    def run(self):
        self._set_config()

        self._set_logger()

        self._create_session()

        # --- INGESTÃO ---
        run_ingestion(self.config, self.spark_session, self.logger)

        # --- TRANSFORMAÇÃO ---
        run_transformations(self.config, self.spark_session, self.logger)