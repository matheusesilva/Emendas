from src import ETLPipeline
from src.utils import get_logger

if __name__ == "__main__":
    # Configura Logger
    logger = get_logger("Main")

    # Instancia e roda
    pipeline = ETLPipeline("emendas_parlamentares")
    pipeline.run()