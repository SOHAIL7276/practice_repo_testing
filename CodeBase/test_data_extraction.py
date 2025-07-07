import pandas as pd
from sqlalchemy import create_engine, text
import cx_Oracle
import logging

# from practice_repo_testing.CodeBase.oracle_test_chatgpt import query

oracle_engine = create_engine("oracle+cx_oracle://system:NewPassword123@localhost:1521/orcl")
mysql_engine_stg =  create_engine("mysql+pymysql://root:Ambreen%407276@localhost:3306/stg_retaildwh")

logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode = 'w',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

def test_dataextraction_from_sales_data_file_to_staging():
    df_expected= pd.read_csv ("SourceSystems/sales_data_linux_remote.csv")
    query_actual = """select * from staging_sales"""
    df_actual = pd.read_sql(query_actual, mysql_engine_stg)
    assert df_actual.equals(df_expected),"data extraction did not happen correctly"


def test_dataextraction_from_product_data_file_to_staging():
    df_expected= pd.read_csv ("SourceSystems/product_data.csv")
    query_actual = """select * from staging_product"""
    df_actual = pd.read_sql(query_actual, mysql_engine_stg)
    assert df_actual.equals(df_expected),"data extraction did not happen correctly"

def test_dataextraction_from_inventory_data_file_to_staging():
    df_expected= pd.read_xml("SourceSystems/inventory_data.xml",xpath=".//item")
    query_actual = """select * from staging_inventory"""
    df_actual = pd.read_sql(query_actual, mysql_engine_stg)
    assert df_actual.equals(df_expected),"data extraction did not happen correctly"

def test_dataextraction_from_supplier_data_file_to_staging():
    df_expected= pd.read_json ("SourceSystems/supplier_data.json")
    query_actual = """select * from staging_supplier"""
    df_actual = pd.read_sql(query_actual, mysql_engine_stg)
    assert df_actual.equals(df_expected),"data extraction did not happen correctly"

# def test_dataextraction_from_store_data_file_to_staging():
#     query_expected = """select * from staging_supplier"""
#     df_expected = pd.read_sql(query_actual, mysql_engine_stg)
#     query_actual = """select * from staging_supplier"""
#     df_actual = pd.read_sql(query_actual, mysql_engine_stg)
#     assert df_actual.equals(df_expected),"data extraction did not happen correctly"

if __name__ == "__main__":
    test_dataextraction_from_sales_data_file_to_staging()
    test_dataextraction_from_product_data_file_to_staging()
    test_dataextraction_from_inventory_data_file_to_staging()
    test_dataextraction_from_supplier_data_file_to_staging()



