import os
import sys
from io import StringIO
from logger import logger
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras
import meta_data as md
import nameparser
import probablepeople
import usaddress
import neo4j
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable


load_dotenv()
local_dev = os.getenv("LOCAL_DEV") == "true"


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

    curs.execute("SELECT name,filing_num FROM master;")

    filing_num_dict = {}

    for bus in curs.fetchall():
        filing_num_dict[bus[0]] = bus[1]
    
    return(filing_num_dict)

def get_charter_officer_data(curs):
        curs.execute("SELECT master.filing_num,name,business_name FROM master INNER JOIN charter_officer_business ON master.filing_num = charter_officer_business.filing_num;")
        return(curs.fetchall())

class COB_Graph_Driver:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)


    @staticmethod
    def _create_and_return_business(tx,filing_num,business_name):
        result = tx.run("Create (a:Business) "
                        "SET a.filing_num = $filing_num "
                        "SET a.name = $business_name "
                        "RETURN id(a)", filing_num = filing_num, business_name = business_name)

        return result.single()[0]


    @staticmethod
    def _create_and_return_cob_relation(tx,business_name,charter_officer_business_name):
        result = tx.run("MATCH"
                        "   (a:business)"
                        "   (b:business)"

                        "WHERE a.name = $business_name AND b.name = $charter_officer_business_name"
                        "CREATE (b)-[r:is_charter_officer_business]->(a) "
                        "RETURN type(r)", charter_officer_business_name = charter_officer_business_name, business_name = business_name)

        return result.single()[0]

    def create_all_nodes(self,cob_node_data_dictionary):
        with self.driver.session() as session:
            i = 0
            for key in cob_node_data_dictionary.keys():
                result = session.write_transaction(self._create_and_return_business,key,cob_node_data_dictionary[key])
                #print(result)
                i +=1 
                if(i % 1000000 ==0):
                    print(i)
               
            
    def create_all_cob_edges(self,cob_data):
        with self.driver.session() as session:
            i = 0
            for entry in cob_data:
                result = session.write_transaction(self._create_and_return_cob_relation,cob_data[1],cob_data[2])
                #print(result)
                i += 1
                if(i % 10000 ==0):
                    print(i)



    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

    def create_friendship(self, person1_name, person2_name):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_and_return_friendship, person1_name, person2_name)
            for record in result:
                print("Created friendship between: {p1}, {p2}".format(
                    p1=record['p1'], p2=record['p2']))

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name):

        # To learn more about the Cypher syntax,
        # see https://neo4j.com/docs/cypher-manual/current/

        # The Reference Card is also a good resource for keywords,
        # see https://neo4j.com/docs/cypher-refcard/current/

        query = (
            "CREATE (p1:Person { name: $person1_name }) "
            "CREATE (p2:Person { name: $person2_name }) "
            "CREATE (p1)-[:KNOWS]->(p2) "
            "RETURN p1, p2"
        )
        result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
        try:
            return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_business(self, business_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_business, business_name)
            for record in result:
                print("Found business: {record}".format(record=record))

    @staticmethod
    def _find_and_return_business(tx, business_name):
        query = (
            "MATCH (b:business) "
            "WHERE b.name = $business_name "
            "RETURN b.name AS name"
        )
        result = tx.run(query, business_name=business_name)
        return [record["name"] for record in result]


if __name__ == "__main__":
    greeter = COB_Graph_Driver("bolt://localhost:7687", "neo4j", "yigith")
    filing_num_dict = get_filing_nums(cursor)
    cob_relations = get_charter_officer_data(cursor)
    #greeter.print_greeting("hello, world")
    #greeter.create_all_nodes(filing_num_dict)
    greeter.create_all_cob_edges(cob_relations)
    greeter.close()