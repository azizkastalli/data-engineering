## Introduction
    The main goal of this project is to move the processes and data of a music streaming app (sparkify) onto the cloud.
    During this project I designed a data-warehouse following the star schema to store the data which will be used for analytics.
    Furthermore, I built an ETL pipeline that extracts their data from S3 buckets and stages them in Redshift, then transformed and load it 
    into the fact and dimension tables.<br>
    The data that resides in S3 contains JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


## Project Content and Structure
### 1. ERD
    
    ![image](https://drive.google.com/uc?export=view&id=1M5xNIgHgMI10vFy1Yl5WaM49qOWFC61u | width=100)

    This data warehouse follows a star schema design such as <b>songplay</b> is the fact table. 
    The dimension tables are as follows:
    * <b>artists:</b> contains information about the songs artists.
    * <b>users:</b> contains information about the users.
    * <b>songs:</b> contains information about songs.
    * <b>time:</b> contains information about the events time.

### 2. Project Structure
    .
    +-- _create_tables.py
    +-- _etl.py
    +-- _sql_queries.py
    +-- _dwh.cfg

    * <b>create_tables.py:</b> This module manages the tables creation in redshift database.
    * <b>etl.py:</b> This module manages the ETL process in redshift database.
    * <b>sql_queries.py:</b> This module contains all necessary SQL queries to drop, create tables and process the ETL process.
    * <b>dwh.cfg:</b> This module contains the s3 buckets links, the redshift cluster credentials and the IAM role, all necessary to make the connections to redshift and s3 buckets. 
    
### 3. How to Run the Python Scripts

    First, execute the command
    ``` python create_tables.py ```
    This command drop all tables in the redshift database and create 
    the tables again as well as all necessary constraints and relationships.
    
    Second, execute the command 
    ``` python etl.py ```
    This command will load staging tables from s3 buckets to redshift database
    and process the ETL pipeline to extract data from the staging tables, then transform and load them into a data the redshift warehhouse.

