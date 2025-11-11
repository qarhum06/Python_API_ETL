import json
import pandas as pd
import pyodbc
from logger_config import setup_logger


logger = setup_logger()

def load_data(df, config_path='config.json'):
    with open(config_path, 'r') as file:
        config = json.load(file)
    
    driver = config.get("driver")
    server = config.get("server")
    database = config.get("database")
    trusted_connection = config.get("TRUSTED_CONNECTION", "yes")
    table = config.get("table")

    conn_str = f"DRIVER={{{driver}}};SERVER={server};TRUSTED_CONNECTION={trusted_connection};"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    try:
        cursor.execute(f"if not exists (select * from sys.databases where name='{database}') create database {database}")
        conn.autocommit = True
    except Exception as e:
        logger.error(f"Error creating database: {e}")
        conn.close()
        return
    conn.close()

    conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};TRUSTED_CONNECTION={trusted_connection};"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    create_table_query = f"""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table}' AND xtype='U')
    CREATE TABLE {table} (
        ProductID INT,
        ProductName NVARCHAR(255),
        UnitPrice FLOAT,
        Quantity INT,
        TotalPrice FLOAT,
        DiscountPercent FLOAT,
        DiscountedPrice FLOAT,
        UserID INT,
        CartTotal FLOAT,
        CartID INT,
        NetRevenue FLOAT,
        Dicounted_Percentage FLOAT,
        SaleCategory NVARCHAR(50)
    )
    """
    try:
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Table {table} is ready.")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.close()
        return
    
    inserted  = 0

    for index, row in df.iterrows():
        try:
            insert_query = f"""
            INSERT INTO {table} (ProductID, ProductName, UnitPrice, Quantity, TotalPrice, DiscountPercent, DiscountedPrice, UserID, CartTotal, CartID, NetRevenue, Dicounted_Percentage, SaleCategory)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, row.ProductID, row.ProductName, row.UnitPrice, row.Quantity, row.TotalPrice, row.DiscountPercent, row.DiscountedPrice, row.UserID, row.CartTotal, row.CartID, row.NetRevenue, row.Dicounted_Percentage, row.SaleCategory
            cursor.execute(*insert_query)
            inserted += 1
        except Exception as e:
            logger.error(f"Error inserting row {index}: {e}")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Inserted {inserted} records into the database table {table}.")




