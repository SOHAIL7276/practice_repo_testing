import pandas as pd
from sqlalchemy import create_engine, text
import cx_Oracle
import logging

# from practice_repo_testing.CodeBase.oracle_test_chatgpt import query

oracle_engine = create_engine("oracle+cx_oracle://system:NewPassword123@localhost:1521/orcl")
mysql_engine_tgt =  create_engine("mysql+pymysql://root:Ambreen%407276@localhost:3306/tgt_retaildwh")

logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode = 'w',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)


def load_fact_sales_table():
    query = text("""insert into fact_sales( sales_id , product_id, store_id,quantity,total_amount,sale_date)
                    select sales_id , product_id, store_id,quantity,total_amount,sale_date 
                    from stag_retaildwh.joiner_sales_products_stores_data""")
    try:
        with mysql_engine_tgt.connect() as conn:
            logger.info("fact_sales table started loading .. ...")
            conn.execute(query)
            conn.commit()
            logger.info("fact_sales table completed loading .. ...")
    except Exception as e:
        logger.error(f"Error encounted while loading fact sales data,{e}", exc_info=True)


def load_fact_inventory_table():
        query = text("""insert into tgt_retaildwh.fact_inventory (last_updated,product_id,quantity_on_hand,store_id)
                        select last_updated,product_id,quantity_on_hand,store_id 
                        from stag_retaildwh.staging_inventory""")
        try:
            with mysql_engine_tgt.connect() as conn:
                logger.info("fact_inventory table started loading .. ...")
                conn.execute(query)
                conn.commit()
                logger.info("fact_inventory table completed loading .. ...")
        except Exception as e:
            logger.error(f"Error encounted while loading fact inventory data,{e}", exc_info=True)


def load_inventory_level_by_store():
    query = text("""insert into tgt_retaildwh.inventory_level_by_store(store_id, total_inventory)
                        select store_id,  total_inventory
                        from stag_retaildwh.aggregator_inventory_data""")
    try:
        with mysql_engine_tgt.connect() as conn:
            logger.info("inventory_level_by_store started loading .. ...")
            conn.execute(query)
            conn.commit()
            logger.info("inventory_level_by_store completed loading .. ...")
    except Exception as e:
        logger.error(f"Error encounted while loading inventory_level_by_store,{e}", exc_info=True)


def load_monthly_sales_summary():
    query = text("""insert into tgt_retaildwh.monthly_sales_summary(product_id,month,year,total_sales)
                        select product_id,month,year,total_sales
                        from stag_retaildwh.aggregator_sales_data""")
    try:
        with mysql_engine_tgt.connect() as conn:
            logger.info("monthly_sales_summary started loading .. ...")
            conn.execute(query)
            conn.commit()
            logger.info("monthly_sales_summary completed loading .. ...")
    except Exception as e:
        logger.error(f"Error encounted while loading monthly_sales_summary,{e}", exc_info=True)

if __name__ == "__main__":
    load_fact_sales_table()
    load_fact_inventory_table()
    load_inventory_level_by_store()
    load_monthly_sales_summary()


