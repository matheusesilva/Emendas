import argparse
from src import ETLPipeline
from src.utils import get_logger, load_config


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executa o pipeline ETL")
    parser.add_argument('--pipeline', type=str, help='Nome da pipeline a ser executada')
    parser.add_argument('--raw-path', type=str, help='Caminho para a camada raw')
    parser.add_argument('--bronze-path', type=str, help='Caminho para a camada bronze')
    parser.add_argument('--silver-path', type=str, help='Caminho para a camada silver')
    parser.add_argument('--gold-path', type=str, help='Caminho para a camada gold')
    parser.add_argument('--url', type=str, help='URL para download dos dados')
    parser.add_argument('--log-level', type=str, help='Nível de logging (ex: INFO, DEBUG, WARNING)')
    args = parser.parse_args()

    config = load_config(args=args)

    logger = get_logger("Main")

    pipeline = ETLPipeline(config.get('default_pipeline'))
    pipeline.run_config = config 
    pipeline.run()