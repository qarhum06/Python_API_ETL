import logging

def setup_logger() :
    logger = logging.getLogger("ETL Logger")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler("etl.log")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger