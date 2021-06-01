This project is meant to build a network graph based on data from the project https://github.com/funkonaut/Sos_data_fun.

1. Set up the Sos_data_fun project, following all directions in the readme: https://github.com/funkonaut/Sos_data_fun. 

2. Once you have the SOS database populated, install neo4j and create run a graph database.

3.  Create a .env file with the same enviorment variables as the Sos_data_fun project 
as well as "GRAPH_DB_PSSWRD  = your_neo4j_password"
(If you are having trouble with neo4j authentication, just comment out the line graph_db_psswrd = os.getenv("GRAPH_DB_PSSWRD")
in build_graphy.py and disable authentication in neo4j).

4. make sure to install apoc plugins in neo4j for the database

5. install required python packages (listed in the requirements.txt file) with pip

6. run the build_graph.py script

This will set up nodes and two relations on a graph database. Other fields from the SOS database need deduplication before rendering on the graph. A sample deduplication is included in this project. To run it, complete the following steps:

1. Make sure to have Spark and Hadoop installed. Installation instructions can be found at (https://spark.apache.org/downloads.html) and (https://hadoop.apache.org/) respectively. 

2. Change the lines 
    conf.set("spark.jars", "C:\\Spark\\spark-3.1.1-bin-hadoop2.7\\jars\\scala-udf-similarity-0.0.7.jar")
    conf.set("spark.jars", "C:\\Spark\\spark-3.1.1-bin-hadoop2.7\\jars\\scala-udf-JaroWinkler-0.0.1.jar")
in demo_utils.py to match your spark installation.

Also, change the lines 
    os.environ["hadoop.home.dir"] =  "C:\\"
    os.environ["HADOOP_HOME"] =  "C:\\Users\\bwrig\\Downloads\\hadoop-3.1.0" #change to your hadoop installation
in splinker.py to match your hadoop installation.

3. Run splinker.py