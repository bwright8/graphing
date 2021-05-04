import os

from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras
import postal_address

import neo4j
from neo4j import GraphDatabase
import logging
import update_TCAD_data
import string_grouper
import altair as alt
alt.renderers.enable('mimetype')

from splink import Splink


logging.basicConfig()  # Means logs will print in Jupyter Lab
logging.getLogger("splink").setLevel(logging.INFO)
os.environ["hadoop.home.dir"] =  "C:\\"
os.environ["HADOOP_HOME"] =  "C:\\Users\\bwrig\\Downloads\\hadoop-3.1.0"

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

if __name__ == "__main__":

    bn = get_business_names(conn)
    
    spark = get_spark()

    settings = {
    "link_type": "dedupe_only",
    "blocking_rules": [
        "l.name = r.name"
    ],
    "comparison_columns": [
        {
            "col_name": "name",
            "term_frequency_adjustments": True},
        {
            "col_name": "filing_num",
            "term_frequency_adjustments": True
        }
    ]
    }


    linker = Splink(settings, bn, spark =  spark)
    
    df_e = linker.get_scored_comparisons()