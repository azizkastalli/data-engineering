### Project Summary:
This project entails building a postgres database with tables to facilitate song play analysis queries. 
The data, which includes songs and user behavior on their new music streaming service, was gathered by Sparkify.
In this project, I created an ETL pipeline that uses Python and SQL to transfer meta-data files into the fact and dimension tables in Postgres.

### ERD
![image](https://drive.google.com/uc?export=view&id=1iAyWEerDMGACxomATLrsdvoSY843A6M0)

### Project's structure:
1. <b>create_tables.py</b>: a python module used to create all necessary tables for the sparkify app.
2. <b>etl.py</b>: a python module used to process the meta-data stored in the data folder and populate the databse.
3. <b>sql_queries.py</b>: a python module containing all necessary sql queries used to create, drop and insert into tables.
4. <b>etl.ipynb</b>: a jupyter notebook used for implementing and cheking the functions used to process the meta-data files.
5. <b>test.ipynb</b>: a jupyter notebook used for sanity check tests.
6. <b>data</b>: a folder that contains all the song and log meta-data in json format.

### How to run the Python scripts
First, run the module create_tables.py following this command:<br>
* python create_tables.py<br>
This module will drop existing tables from the database, then creates the tables needed for the sparkify app 
as well as their relationships.

Next, run the module etl.py following this command:<br>
* python etl.py<br>
This modlule will process song and log json files and populate the databse.