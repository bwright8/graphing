import os

from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras

import neo4j
from neo4j import GraphDatabase
import logging


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

def get_filing_nums(curs):


    # curs.execute("SELECT name,filing_num FROM master LIMIT 50000;")
    #for testing purposes

    curs.execute("SELECT name,filing_num FROM master;")


    filing_num_dict = {}

    for bus in curs.fetchall():
        filing_num_dict[bus[0]] = bus[1]
    
    return(filing_num_dict)

def get_charter_officer_data(curs):
        curs.execute("SELECT master.filing_num,name,business_name FROM master INNER JOIN charter_officer_business ON master.filing_num = charter_officer_business.filing_num;")
        return(curs.fetchall())

class Graph_Driver:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()



    @staticmethod
    def _create_and_return_business(tx,filing_num,business_name):
        #creation of a business-type node
        result = tx.run("Create (a:Business) "
                        "SET a.filing_num = $filing_num "
                        "SET a.name = $business_name "
                        "RETURN id(a)", filing_num = filing_num, business_name = business_name)

        return result.single()[0]


    @staticmethod
    def _create_and_return_cob_relation(tx,fn1,fn2):
        #creation of a charter-officer-business type relation
        result = tx.run("MATCH"
                        "   (a:Business), "
                        "   (b:Business) "

                        "WHERE a.filing_num = $fn1 AND b.filing_num = $fn2 "
                        "CREATE (b)-[r:is_charter_officer_business]->(a) "
                        "RETURN type(r)", fn1 = fn1, fn2 = fn2)

        return result.single()

    def create_all_nodes(self,cob_node_data_dictionary):
        #method to create all nodes for the graph, to populate it
        with self.driver.session() as session:
            i = 0
            for key in cob_node_data_dictionary.keys():
                result = session.write_transaction(self._create_and_return_business,cob_node_data_dictionary[key],key)
                #print(result)
                i +=1 
                if(i % 1000000 ==0):
                    print(i)
            result = session.write_transaction(self._create_filing_number_index)
            print(result)
            
    def create_all_cob_edges(self,cob_data,filing_num_dict):

        #method to populate charter-officer-business edges for the graph
        
        with self.driver.session() as session:
            i = 0
            for entry in cob_data:
                try:
                    thing1 = filing_num_dict[entry[1]]
                    thing2 = filing_num_dict[entry[2]]

                    result = session.write_transaction(self._create_and_return_cob_relation,thing1,thing2)
                    #print(result)
                   
                except:
                   pass

                i += 1
                if(i % 100000 ==0):
                    print(i) 
                    print(result)




    def find_business(self, business_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_business, business_name)
            for record in result:
                print("Found business: {record}".format(record=record))

    @staticmethod
    def _create_filing_number_index(tx):
        #create an index on filing_num for fast matching
        query = ("CREATE INDEX fn_index IF NOT EXISTS FOR (n:Business) ON (n.filing_num)")
        result = tx.run(query)
        return(result)


    @staticmethod
    def _find_and_return_business(tx, business_name):
        query = (
            "MATCH (b:Business) "
            "WHERE b.name = $business_name "
            "RETURN b.name AS name"
        )
        result = tx.run(query, business_name=business_name)
        return [record["name"] for record in result]


if __name__ == "__main__":
    graph_driver = Graph_Driver("bolt://localhost:7687", "neo4j", graph_db_psswrd)
    master_filing_num_dict = get_filing_nums(cursor)
    cob_relations = get_charter_officer_data(cursor)
    graph_driver.create_all_nodes(master_filing_num_dict)
    graph_driver.create_all_cob_edges(cob_relations, master_filing_num_dict)
    graph_driver.close()
