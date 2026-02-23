from pyspark.sql import SparkSession
from src.bronze_ingestion import run_ingestion
from src.silver_transformation import run_transformations
# from gold_delivery import run_delivery # Ainda não implementado
from src.utils import Config, create_logger

class ETLPipeline:
    def __init__(self, args=None):
        self.args = args
        self.config = None
        self.logger = None
        self.spark_session = None

    def _set_config(self):
        # Usa config via CLI se fornecida, senão carrega do arquivo YAML
        self.config = Config(args=self.args)

    def _set_logger(self):
        self.logger = create_logger(self.config)

    def _create_session(self):
        self.spark_session = SparkSession.builder.appName(self.config.pipeline_name).getOrCreate()

    def run(self):
        """Executa o pipeline completo: Ingestão -> Transformação -> Entrega"""
        self._set_config()
        self._set_logger()
        self._create_session()

        # --- INGESTÃO ---
        run_ingestion(self.config, self.spark_session, self.logger)

        # --- TRANSFORMAÇÃO ---
        run_transformations(self.config, self.spark_session, self.logger)