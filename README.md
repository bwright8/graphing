This project is meant to build a network graph based on data from the project https://github.com/funkonaut/Sos_data_fun.

1. Set up the Sos_data_fun project, following all directions in the readme: https://github.com/funkonaut/Sos_data_fun. 

2. Once you have the SOS database populated, install neo4j and create run a graph database.

3.  Create a .env file with the same enviorment variables as the Sos_data_fun project 
as well as "GRAPH_DB_PSSWRD  = your_neo4j_password"
(If you are having trouble with neo4j authentication, just comment out the line graph_db_psswrd = os.getenv("GRAPH_DB_PSSWRD")
in build_graphy.py and disable authentication in neo4j).

4. make sure to install apoc plugins in neo4j for the database

5. run the build_graph.py script