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

from build_graph import *

load_dotenv()
local_dev = os.getenv("LOCAL_DEV") == "true"
graph_db_psswrd = os.getenv("GRAPH_DB_PSSWRD")


if __name__ == '__main__':

    #graph_driver = Graph_Driver("bolt://localhost:7687", "neo4j", graph_db_psswrd)
    
    df = update_TCAD_data.read_tcad()
