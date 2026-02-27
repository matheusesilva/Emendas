from pyspark.sql import SparkSession
from src.bronze_ingestion import get_zip, run_ingestion
from src.silver_transformation import run_transformations
from src.gold_delivery import run_delivery
from src.utils import Config, get_logger

class ETLPipeline:
    def __init__(self, args=None):
        self.args = args
        self.config = None
        self.logger = None
        self.spark_session = None

    def _set_config(self):
        self.config = Config(self.args)

    def _set_logger(self):
        self.logger = get_logger(self.config)

    def _create_session(self):
        self.spark_session = SparkSession.builder \
            .appName(self.config.name) \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
    
    def _stop_session(self):
        if self.spark_session:
            self.spark_session.stop()

    def run(self):
        """Executa o pipeline completo: Ingestão -> Transformação -> Entrega"""
        self._set_config()
        self._set_logger()

        # --- GET RAW DATA ---
        zip_path = get_zip(self.config, self.logger)

        # --- INGESTÃO ---
        self._create_session()
        run_ingestion(self.config, self.spark_session, self.logger, zip_path)

        # --- TRANSFORMAÇÃO ---
        run_transformations(self.config, self.spark_session, self.logger)
        self._stop_session()

        # --- ENTREGA ---
        run_delivery(self.config, self.logger)