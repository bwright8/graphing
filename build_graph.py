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

def get_registered_agent_business_data(curs):
    curs.execute("SELECT master.filing_num,name,business_name FROM master INNER JOIN registered_agent_business ON master.filing_num = registered_agent_business.filing_num;")
    return(curs.fetchall())

def get_address_book(curs):
    curs.execute("SELECT * FROM address;")
    return(curs.fetchall())       

def get_corp_type_ids(curs):
    curs.execute("SELECT filing_num, corp_type_id FROM master;")
    return(curs.fetchall()) 

def get_tcad_data():
    df = update_TCAD_data.read_tcad()
    return df

def make_address_groups(address_book):
    pas = []
    for row in address_book:
        fn = row[0]
        address1 = row[2]
        #address2 = row[3]
        city = row[4]
        state = row[5]
        zip_code = row[6]
        zip_extension = row[7]
        country = row[8]
        pa = postal_address.address.Address(line1 = address1,
            #line2 = address2,
            city_name = city,
            country_code = country,postal_code = zip_code).render()

        pas.append(pa)

    groups = string_grouper.string_grouper.group_similar_strings(pd.Series(pas))
    print(groups[0:10])
    return groups



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
            result = session.write_transaction(self._create_business_name_index)

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


    @staticmethod
    def _create_and_return_rab_relation(tx,fn1,fn2):
        #creation of a rab type relation
        result = tx.run("MATCH"
                        "   (a:Business), "
                        "   (b:Business) "

                        "WHERE a.filing_num = $fn1 AND b.filing_num = $fn2 "
                        "CREATE (b)-[r:is_registered_agent_business]->(a) "
                        "RETURN type(r)", fn1 = fn1, fn2 = fn2)

        return result.single()


    def create_all_rab_edges(self,rab_data,filing_num_dict):

        #method to populate registered-agent-business edges for the graph
        
        with self.driver.session() as session:
            i = 0
            for entry in rab_data:
                try:
                    thing1 = filing_num_dict[entry[1]]
                    thing2 = filing_num_dict[entry[2]]

                    result = session.write_transaction(self._create_and_return_rab_relation,thing1,thing2)
                    #print(result)
                   
                except:
                   pass

                i += 1
                if(i % 100000 ==0):
                    print(i) 
                    print(result)


    @staticmethod
    def _create_and_return_prop_owner_relation(tx,fn1,address1,zip_code,pa):
        #creation of a property_owner type relation
        result = tx.run("MATCH "
                        "(a:Business) "
                        "WHERE a.filing_num = $fn1 "
                        "MATCH  (b:Address) WHERE  b.pa = $pa "
                        "CREATE (a)-[r:is_property_owner]->(b) "
                        "RETURN type(r)",fn1 = fn1, address1 = address1, zip_code = zip_code,pa = pa)

        return result.single()


    def create_prop_owner_edges(self,prop_data,filing_num_dict):

        #method to populate prop-owner-business edges for the graph
        
        with self.driver.session() as session:
            i = 0
            k = 0
            for j in range(len(prop_data)):
                try:

                    busi = filing_num_dict[prop_data['prop_owner'][j]]

                    address1 = prop_data['mail_add_2'][j]
                    address2 = prop_data['mail_add_3'][j]
                    zip_code = prop_data['mail_zip'][j]
                    city = prop_data['mail_city'][j]
                    # state = prop_data['mail_state']
                    
                    pa = postal_address.address.Address(line1 = address1,line2 = address2,city_name = city,country_code = "US",
                        postal_code = zip_code).render()
                   

                    result = session.write_transaction(self._create_and_return_prop_owner_relation,busi,address1,zip_code,pa)
                    #print(result)
                    i += 1
                    if(i % 10000 ==0):
                        print(i) 
                        print(result)
                except:
                    k += 1
                    if(k % 10000 ==0):
                        print(j) 

                




    def find_business(self, business_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_business, business_name)
            for record in result:
                print("Found business: {record}".format(record=record))


    def update_addresses(self,address_book,address_groups):
        
        with self.driver.session() as session:
            i = 0
            
            
            for i in range(len(address_book)):
                row = address_book[i]
                gp = address_groups[i]
                fn = row[0]
                address1 = row[2]
                #address2 = row[3]
                city = row[4]
                state = row[5]
                zip_code = row[6]
                zip_extension = row[7]
                country = row[8]
                pa = postal_address.address.Address(line1 = address1,
                    #line2 = address2,
                    city_name = city,
                    country_code = country,postal_code = zip_code).render()
                result = session.write_transaction(self._update_address,fn,
                    address1,
                    #address2,
                    city,state,zip_code,zip_extension,
                    country,pa,gp)
                #print(result)
                i +=1 
                if(i % 1000000 ==0):
                    print(i)
                    print(result)
            #result = session.write_transaction(self._merge_addresses)

            result = session.write_transaction(self._create_address1_index)
            result = session.write_transaction(self._create_zip_code_index)
            result = session.write_transaction(self._create_pa_index)
            result = session.write_transaction(self._create_gp_index)
            print(result)
    
    def update_corp_type_ids(self,corp_ids):

        with self.driver.session() as session: 
            i = 0
            
            for row in corp_ids:
                fn = row[0]
                id = row[1]
                result = session.write_transaction(self._update_corp_type_id,fn,id)
                #print(result)
                i +=1 
                if(i % 1000000 ==0):
                    print(i)
                    print(result)
            

    @staticmethod
    def _create_filing_number_index(tx):
        #create an index on filing_num for fast matching
        query = ("CREATE INDEX fn_index IF NOT EXISTS FOR (n:Business) ON (n.filing_num)")
        result = tx.run(query)
        return(result)

    @staticmethod
    def _create_business_name_index(tx):
        #create an index on name for fast matching
        query = ("CREATE INDEX name_index IF NOT EXISTS FOR (n:Business) ON (n.name)")
        result = tx.run(query)
        return(result)

    @staticmethod
    def _create_address1_index(tx):
        #create an index on address for fast matching
        query = ("CREATE INDEX add1_index IF NOT EXISTS FOR (n:Address) ON (n.address1)")
        result = tx.run(query)
        return(result)

    @staticmethod
    def _create_zip_code_index(tx):
        #create an index on zip for fast matching
        query = ("CREATE INDEX zip_index IF NOT EXISTS FOR (n:Address) ON (n.zip_code)")
        result = tx.run(query)
        return(result)

    @staticmethod
    def _create_pa_index(tx):
        #create an index on pa for fast matching
        query = ("CREATE INDEX pa_index IF NOT EXISTS FOR (n:Address) ON (n.pa)")
        result = tx.run(query)
        return(result)
    
    @staticmethod
    def _create_gp_index(tx):
        #create an index on gp for fast matching
        query = ("CREATE INDEX gp_index IF NOT EXISTS FOR (n:Address) ON (n.gp)")
        result = tx.run(query)
        return(result)
    

    @staticmethod
    def _merge_addresses(tx,address_book):
        query = ("MATCH (A)-[r:is_at]->(B) "
            "WITH  count(r) as relsCount "
            "MATCH (A)-[r:is_at]->(B) "
            "WHERE relsCount > 1 "
            "WITH A,B,collect(r) as rels "
            "CALL apoc.refactor.mergeRelationships(rels,{properties:'combine'}) "
            "YIELD rel RETURN rel")

        query = ("MATCH (a:Address) "
            "WITH a.pa as pa "
            "COLLECT(a) as nodelist, COUNT(*) as count "
            "WHERE count > 1 "
            "CALL apoc.refactor.mergeNodes(nodelist) yield node return node "
        )
        i = 0
        for a in address_book:
                address1 = a[2]
                address2 = a[3]
                city = a[4]
                state = a[5]
                zip_code = a[6]
                zip_extension = a[7]
                country = a[8]
                pa = postal_address.address.Address(line1 = address1,city_name = city,
                    country_code = country,postal_code = zip_code).render()

                query = ("MATCH (a:Address {pa :$pa}) "
                    "WITH COLLECT(a) AS nodes "
                    "CALL apoc.refactor.mergeNodes(nodes) "
                    "YIELD node "
                    "RETURN node")
                result = tx.run(query, pa = pa)
                i += 1
                if( i  % 100000 == 0): 
                    print(i)

        #result = tx.run(query)
        return(result)

    def merge_addresses(self,address_book):
        with self.driver.session() as session:
            result = session.write_transaction(self._merge_addresses,address_book)

    @staticmethod
    def _find_and_return_business(tx, business_name):
        query = (
            "MATCH (b:Business) "
            "WHERE b.name = $business_name "
            "RETURN b.name AS name"
        )
        result = tx.run(query, business_name=business_name)
        return [record["name"] for record in result]

    @staticmethod
    def _update_corp_type_id(tx,fn,id):
        query = (
            "MATCH (b:Business) "
            "WHERE b.filing_num = $fn "
            "SET b.corp_type_id = $id "
            "RETURN b"
        )
        result = tx.run(query,fn = fn, id = str(id))
        return(result)

    @staticmethod
    def _update_address(tx,fn,address1,address2,city,state,zip_code,
            zip_extension,country,pa,gp):

        query = (
            "MATCH (s:Business) "
            "WHERE s.filing_num = $fn "
            "MERGE (b:Address {gp:$gp}) "
            "SET b.address1 = $address1 "
            "SET b.address2 = $address2 "
            "SET b.city = $city "
            "SET b.state = $state "
            "SET b.zip_code = $zip_code "
            "SET b.zip_extension = $zip_extension "
            "SET b.country = $country "
            "SET b.pa = $pa "
            "CREATE (s)-[r:is_at]->(b)"
            "RETURN r,b"
        )
        result = tx.run(query,fn = fn, address1 = address1,
            address2 = address2, city = city, state = state, 
            zip_code = str(zip_code),zip_extension = str(zip_extension),
            country = country,pa = pa,gp = gp)
        return(result)
        

if __name__ == "__main__":
    graph_driver = Graph_Driver("bolt://localhost:7687", "neo4j", graph_db_psswrd)
    
    master_filing_num_dict = get_filing_nums(cursor)

   
    """
    cob_relations = get_charter_officer_data(cursor)
    graph_driver.create_all_nodes(master_filing_num_dict)
    graph_driver.create_all_cob_edges(cob_relations, master_filing_num_dict)
    

    
    corp_ids = get_corp_type_ids(cursor)
    graph_driver.update_corp_type_ids(corp_ids)
    """
    
    address_book = get_address_book(cursor)
    address_groups = make_address_groups(address_book)

    
    
    #graph_driver.update_addresses(address_book)
    
    """
    rab_relations = get_registered_agent_business_data(cursor)
    graph_driver.create_all_rab_edges(rab_relations,master_filing_num_dict)
    """
    """
    df = get_tcad_data()
    graph_driver.create_prop_owner_edges(df,master_filing_num_dict)
    """
    #graph_driver.merge_addresses(address_book)
    
    graph_driver.close()