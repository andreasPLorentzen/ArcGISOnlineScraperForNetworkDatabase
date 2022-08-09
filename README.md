# ArcGIS Online ScraperFor Network Database
This script/ system scrapes a series of ArcGIS Online and Enterprise portals and creates input CSV files for use in Neo4J. The project was a pet project, and therefore not fully finished. Might come in v2? who knows?

<h1>How it works:</h1>
This codebase works quite badly, however it scrapes all arcgis portals and online organizations in a list. It gathers email entities, users, items, maps and groups. It exports csv files for Nodes and connections to be used in Neo4j (or other network databases).
I have to stress that this is by far the first version done in a christmas holiday...

<h3>setup</h3>
* Starts by filling the connection-data in the Neo4j_general_functions.py.
* Fill out the rest of the input data in Neo4j_general_functions.py


* Run the "Users and groups.py" script. this creates nodes and connections for all users and groups, and is used by the "items.py" later.
* Run the "Items.py" script. This takes some time as its finds all users items (that are visible to the login user) og goes through all maps, apps etc. and finds connections to other datasets etc.
* Write your Cypher/ CQL code in Neo4J and import everything.
* ??
* profit.

Remember to run the scripts in an environment with arcpy. This was written in arcgis/arcpy v. 2.3 and is tested in 2.7.



