import os

from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras

import neo4j
from neo4j import GraphDatabase
import logging
import update_TCAD_data
import altair as alt
alt.renderers.enable('mimetype')
from splink import Splink


logging.basicConfig()  # Means logs will print in Jupyter Lab
logging.getLogger("splink").setLevel(logging.INFO)
os.environ["hadoop.home.dir"] =  "C:\\"
os.environ["HADOOP_HOME"] =  "C:\\Users\\bwrig\\Downloads\\hadoop-3.1.0" #change to your hadoop instalation

os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[2] pyspark-shell'


from demo_utils import get_spark

load_dotenv()
local_dev = os.getenv("LOCAL_DEV") == "true"
graph_db_psswrd = os.getenv("GRAPH_DB_PSSWRD")



def get_database_connection(local_dev=True):
    """Connection to PSQL DB."""
    if local_dev:
        conn = psycopg2.connect(os.getenv("LOCAL_DATABASE_URL"))
    else:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    return conn

conn = get_database_connection(local_dev=local_dev)
cursor = conn.cursor()

def get_business_names(conn):
    sqlq = "SELECT filing_num, name FROM master;"
    df = pd.read_sql_query(sqlq, conn)

    return df

def get_addresses(conn):
    sqlq = "SELECT filing_num,address1,zip_code,city FROM address;"
    df = pd.read_sql_query(sqlq,conn)

    return df

if __name__ == "__main__":

    bn = get_business_names(conn)
    bn['unique_id'] = bn.filing_num.map(hash)
    

    spark = get_spark()
    bn = spark.createDataFrame(bn)
    settings = {
    "link_type": "dedupe_only",
    "additional_columns_to_retain":[
        "filing_num"
    ],
    
    "comparison_columns": [
        {
            "col_name": "name",
            "term_frequency_adjustments": True},
      
    ],

    "blocking_rules":[
        "l.name = r.name"
    ]

    }
    
    linker = Splink(settings, df_or_dfs = bn, spark =  spark)
    
    df_e = linker.get_scored_comparisons()

    print(df_e.head(10))
    print(df_e.count())
    print(df_e.columns)
    
    """

    ad = get_addresses(conn)
    ad['unique_id'] = (ad.address1+ad.filing_num).map(hash)
    ad = spark.createDataFrame(ad)
    settings = {
    "link_type": "dedupe_only",
    "additional_columns_to_retain":[
        "filing_num"
    ],
    
    "blocking_rules":[

        "l.zip_code = r.zip_code",
        "l.city = r.city",
        "l.address1 = r.address1"



    ],
    "comparison_columns": [
        {
            "col_name": "address1",
            "term_frequency_adjustments": True},
            {
            "col_name": "city",
            "term_frequency_adjustments": True},
             {
            "col_name": "zip_code",
            "term_frequency_adjustments": True}
      
    ]

    }

    linker = Splink(settings, df_or_dfs = ad, spark =  spark)
    
    df_e = linker.get_scored_comparisons()

    print(df_e.head(10))
    print(df_e.count())
    print(df_e.columns)
    """