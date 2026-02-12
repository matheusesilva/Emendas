import requests
import logging
import zipfile
import os
from datetime import datetime

DEFAULT_URL = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/emendas-parlamentares/EmendasParlamentares.zip"
ORIGINAL_DATA_PATH = f"data/raw/original/emendas_parlamentares_{datetime.now().strftime('%Y%m%d')}.zip"
EXTRACTED_DATA_PATH = "data/raw/extracted/"

logger = logging.getLogger(__name__)

def _download_zip(url, download_path):
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an error for HTTP errors
        with open(download_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"Successfully downloaded zip file from {url} to {download_path}")
    except requests.RequestException as e:
        logger.error(f"Error downloading zip file: {e}")
        raise

def _extract_zip(dowload_path, extracted_data_path):
    # Ensure the extraction directory exists
    daily_ingestion_folder = os.path.join(extracted_data_path , datetime.now().strftime('%Y%m%d'))
    os.makedirs(daily_ingestion_folder, exist_ok=True)
    try:
        with zipfile.ZipFile(dowload_path, 'r') as zip_ref:
            zip_ref.extractall(daily_ingestion_folder)
        logger.info(f"Successfully extracted zip file from {dowload_path} to {daily_ingestion_folder}")
    except zipfile.BadZipFile as e:
        logger.error(f"Error extracting zip file: {e}")
        raise

def ingest_data(
        url=DEFAULT_URL,
        download_path=ORIGINAL_DATA_PATH,
        extracted_data_path=EXTRACTED_DATA_PATH
        ):
    _download_zip(url, download_path)
    _extract_zip(download_path, extracted_data_path)
    return True