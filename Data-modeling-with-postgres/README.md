### Project Summary:
This project entails building a postgres database with tables to facilitate song play analysis queries. 
The data, which includes songs and user behavior on their new music streaming service, was gathered by Sparkify.
In this project, I created an ETL pipeline that uses Python and SQL to transfer meta-data files into the fact and dimension tables in Postgres.

### ERD
![image](https://drive.google.com/uc?export=view&id=1iAyWEerDMGACxomATLrsdvoSY843A6M0)

### Project's structure:
```
    .
    ├── create_tables.py          # Creates all necessary tables for the sparkify app.
    ├── etl.py                    # Processes the meta-data stored in the data folder and populate the databse.
    ├── sql_queries.py            # Contains all necessary sql queries used to create, drop and insert into tables.
    ├── etl.ipynb                 # Implements and checks the functions used to process the meta-data files. 
    ├── test.ipynb                # Sanity check tests notebook. 
    ├── data                      # Contains all the song and log meta-data in json format. 
    └── README.md
```

### How to run the Python scripts
First, run the module create_tables.py following this command:<br>
```
python create_tables.py
```
This module will drop existing tables from the database, then creates the tables needed for the sparkify app 
as well as their relationships.

Next, run the module etl.py following this command:<br>
```
python etl.py
```
This modlule will process song and log json files and populate the databse.
