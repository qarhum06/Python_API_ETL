from extract import extract_data
from transform import transform_data
from load import load_data
from logger_config import setup_logger
import json


logger = setup_logger()

def main():
    logger.info("ETL process started.")

    with open('config.json', 'r') as file:
        config = json.load(file)
    
    api_url = config["apiurl"]

    df_extracted = extract_data(api_url)
    if df_extracted.empty:
        logger.warning("No data extracted. ETL process terminated.")
        return
    
    df_transformed = transform_data(df_extracted)

    load_data(df_transformed)

    logger.info("ETL process completed.")

if __name__ == "__main__":
    main()