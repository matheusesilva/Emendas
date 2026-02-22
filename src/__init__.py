from pyspark.sql import SparkSession
from src.bronze_ingestion import run_ingestion
from src.silver_transformation import run_transformations
# from gold_delivery import run_delivery # Ainda não implementado
from src.utils import get_logger, load_config

class ETLPipeline:
    def __init__(self, pipeline_name="ETL Pipeline"):
        self.spark = SparkSession.builder.appName(pipeline_name).getOrCreate()
        self.name = pipeline_name

    def run(self):
        # Inicia logger
        logger = get_logger("ETLPipeline")

        # Carrega configuração
        config = load_config()
        if self.name not in config['pipelines']:
            logger.error(f"CONFIG    | Pipeline '{self.name}' não encontrada em config.yaml.")
            return

        # 1. Ingestão
        run_ingestion(self.name, config, self.spark)

        # 2. Transformação
        run_transformations(self.name, config, self.spark)