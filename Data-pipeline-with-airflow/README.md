## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.<br>
To do so I have to create a high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.<br>
Also, I have to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.<br>

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

During this project, I developped a data pipeline that executes the following steps:<br>
    1. Create the data-model (if it does not exist) on redshift
    2. Stage the data from s3 buckets to redshift (data extraction).
    3. Transform the data into the appropriate data-model (data transformation)
    4. Load the data into redshift using hooks and custom operators (data loading)
    5. Check the data quality by running serveral tests.


## Project Content and Structure
### 1. ERD

![image](https://drive.google.com/uc?export=view&id=1M5xNIgHgMI10vFy1Yl5WaM49qOWFC61u)

This data warehouse follows a star schema design such as <b>songplay</b> is the fact table. 
The dimension tables are as follows:
* <b>artists:</b> contains information about the songs artists.
* <b>users:</b> contains information about the users.
* <b>songs:</b> contains information about songs.
* <b>time:</b> contains information about the events time.

### 2. Project Structure
```
    .
    ├── dags
    |   └── pipeline_dag.py   #contains the main dag.
    ├── plugins
    |   ├── helpers
    |   |    └── sql_queries.py  #contains insert queries to populate the data warehouse.
    |   ├── operators 
    |   |   ├── data_quality.py  #executes data quality checks.
    |   |   ├── load_dimensions.py  #loads data into dimension tables.
    |   |   ├── load_fact.py  #loads data into the fact table.
    |   |   └── stage_redshift.py  #stage data from s3 to redshift (data extraction).
    ├── create_tables.sql  #SQL queries to create dimension and fact tables (data-model)         
    ├── dataquality_check.json   #JSON script for data quality checks.
    └── README.md
```

### 3. How to Run the Python Scripts
#### 3.1 Pre-requesities:
<b>Prerequisites:</b>
* Create an IAM User in AWS.
* Create a redshift cluster in AWS. Ensure that you are creating this cluster in the us-west-2 region. This is important as the s3-bucket that we are going to use for this project is in us-west-2.
* Setting up Connections:
    * Connect Airflow and AWS by creating an amazon web service connection.
    * Connect Airflow to the AWS Redshift Cluster by creating a postgres connection.

#### 3.2 Data:
Here are the s3 links (buckets located in us-west-2 region):
* Log data: s3://udacity-dend/log_data
* Song data: s3://udacity-dend/song_data

#### 3.2 Run the application:
First, execute the command
``` 
/opt/airflow/start.sh 
```
This command will run the apache airflow server.

Second, access the server via your browser and run the dag.

### 4. DAG
![image](https://drive.google.com/uc?export=view&id=1RQxeY-GkPzqXmym7Cd7SoSfhnIDdVb5D)


