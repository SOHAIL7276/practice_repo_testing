import pandas as pd
from sqlalchemy import create_engine, text
import cx_Oracle
import logging

# from practice_repo_testing.CodeBase.oracle_test_chatgpt import query

oracle_engine = create_engine("oracle+cx_oracle://system:NewPassword123@localhost:1521/orcl")
mysql_engine_tgt =  create_engine("mysql+pymysql://root:Ambreen%407276@localhost:3306/stag_retaildwh")

logging.basicConfig(
    filename='Print_Logging/Extraction.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)


def load_fact_sales_table():
    query = text("""insert into fact_sales( sales_id , product_id, store_id,quantity,total_amount,sale_date)
                    select sales_id , product_id, store_id,quantity,total_amount,sale_date from joiner_sales_products_stores_data""")
    try:
        with mysql_engine_tgt.connect() as conn:
            logger.info("fact_sales table started loading .. ...")
            conn.execute(query)
            conn.commit()
            logger.info("fact_sales table completed loading .. ...")
    except Exception as e:
        logger.error(f"Error encounted while loading fact sales data,{e}", exc_info=True)


def load_fact_inventory_table():
        query = text("""insert into fact_inventory (last_updated,product_id,quantity_on_hand,store_id)
                        select last_updated,product_id,quantity_on_hand,store_id from staging_inventory""")
        try:
            with mysql_engine_tgt.connect() as conn:
                logger.info("fact_inventory table started loading .. ...")
                conn.execute(query)
                conn.commit()
                logger.info("fact_inventory table completed loading .. ...")
        except Exception as e:
            logger.error(f"Error encounted while loading fact inventory data,{e}", exc_info=True)


def load_fact_inventory_table():
    query = text("""insert into fact_inventory (last_updated,product_id,quantity_on_hand,store_id)
                        select last_updated,product_id,quantity_on_hand,store_id from staging_inventory""")
    try:
        with mysql_engine_tgt.connect() as conn:
            logger.info("fact_inventory table started loading .. ...")
            conn.execute(query)
            conn.commit()
            logger.info("fact_inventory table completed loading .. ...")
    except Exception as e:
        logger.error(f"Error encounted while loading fact inventory data,{e}", exc_info=True)




if __name__ == "__main__":
    load_fact_sales_table()
    load_fact_inventory_table()


