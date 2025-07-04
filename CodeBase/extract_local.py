import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging


oracle_engine = create_engine("oracle+cx_oracle://system:NewPassword123@localhost:1521/orcl")
mysql_engine =  create_engine("mysql+pymysql://root:Ambreen%407276@localhost:3306/stag_retaildwh")

logging.basicConfig(
    filename='Print_Logging/Extraction.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

'''

def extract_sales_data_from_file():
    try:
        logger.info("sales data extraction started.....")
        df = pd.read_csv ("SourceSystems/sales_data_linux_remote.csv")
        df.to_sql("staging_sales", mysql_engine,if_exists='replace',index=False)
        logger.info("sales data extraction completed.....")

    except Exception as e:
        logger.error(f"Error encounted while extrcting the product data file,{e}",exc_info=True)



def extract_product_data_from_file():
    try:
        logger.info("product data extraction started.....")
        df = pd.read_csv ("SourceSystems/product_data.csv")
        df.to_sql("staging_product", mysql_engine,if_exists='replace',index=False)
        logger.info("product data extraction completed.....")

    except Exception as e:
        logger.error(f"Error encounted while extrcting the product data file,{e}",exc_info=True)


def extract_supplier_data_from_file():
    try:
        logger.info("supplier data extraction started.....")
        df = pd.read_json ("SourceSystems/supplier_data.json")
        df.to_sql("staging_supplier", mysql_engine,if_exists='replace',index=False)
        logger.info("supplier data extraction completed.....")

    except Exception as e:
        logger.error(f"Error encounted while extrcting the product data file,{e}",exc_info=True)


def extract_inventory_data_from_file():
    try:
        logger.info("inventory data extraction started....")
        df = pd.read_xml("SourceSystems/inventory_data.xml",xpath=".//item")
        df.to_sql("staging_inventory",mysql_engine,if_exists='replace',index=False)
        logger.info("inventory data extraction completed....")
    except Exception as e:
        logger.error(f"Error encounted while extrcting the inventory data file,{e}",exc_info=True)

'''


def extract_stores_data_from_oracle():
    try:
        logger.info("stores data extraction started....")
        query = """select * from STORES_CITY"""
        df = pd.read_sql(query,oracle_engine)
        df.to_sql("staging_stores_city", mysql_engine, if_exists='replace', index=False)
        print("Inserted rows to MySQL:", len(df))
        logger.info("stores data extraction completed....")
    except Exception as e:
        logger.error(f"Error encounted while extrcting the stores data from oracle,{e}",exc_info=True)


# extract_sales_data_from_file()
# extract_product_data_from_file()
# extract_supplier_data_from_file()
# extract_inventory_data_from_file()
extract_stores_data_from_oracle()



