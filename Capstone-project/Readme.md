## Data Engineering Capstone Project
### Project Summary
The project consists of creating a data model following a star schema that contains data about immigrants moving to/from US cities. The data model will contain not only information about immigrants but also information about US cities demographics as well as their airports.<br>
The main challenge of this project is to clean and transform data from different sources to generate a data model that represents a unique ground truth about the data and that will be used for analytical processes.<br>
Once the data model is ready for processing, we can get insights such as what cities are prefered by immigrants and we can even filter by age, race and many other dimensions. We can also check if the available airports in the cities can handle the flow of immigrants.<br>

### 1. Scope the Project and Gather Data
#### 1.1 Scope
During the next steps, I will read the data and check if there are any anomalies such as missing data and duplicates.<br> Next, I will define a data model following a star schema, then, I will run data pipelines to aggregate the data by cities and generate dimension and fact tables.<br> Finally, I will check the quality of the data and write the tables as parquet files on S3 which is going to be the final form or output of this project. During these steps I will use pandas and pyspark for data processing. pyspark will be extremely helpful especially when processing the immigration dataset which contains over 3 million rows.

#### 1.2 Data Description
Our data is derived from 3 sources or files as follows:
* [I94 Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html): This data comes from the US National Tourism and Trade Office. It contains detailed information about visitors arrival/departure to/from US states during the period of april 2016.
* [U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/): This data comes from OpenSoft. It contains detailed demographic statistics for each state and city in the US.
* [Airport Code Table](https://datahub.io/core/airport-codes#data): This data contains table of airport codes and corresponding cities.



### 2. Data Model
![image](https://drive.google.com/uc?export=view&id=1krP58ZqA2qFRXoXVvoxK-s8Cc18YBDO7)

The data model is a star schema model. It consists of 3 dimension tables:<br>
- dim_cities_airport: contains airport information aggregated by cities.
- dim_cities_demographics: contains statistical data about cities demographics and aggregated by cities.
- dim_time: contains features extracted from the immigration dates such as year, month, weekday etc.<br>
As well as a fact table (fact_immigration) which contains information about each immigrant.<br>

The star schema model was the preferred choice as we can get analytical insights using a few joins.<br>
Also, the aggregation by cities of both dim_cities_airport and dim_cities_demographics dimension tables was a good choice for this model, as it is the only possible way to link the immigration data with both dimensions (airports and cities demographics).<br>
Aggregating airports by cities made it possible to link each row from the fact table (extracted from the immigration dataset) with each row from both dim_cities_airport and dim_cities_demographics dimension tables.<br>


### 3. Project Write Up
- Choice of tools and technologies for the project:
    - During this project I used Pandas and Pyspark to process our data. I used both tools mainly due to the volume of the data. For example, the immigration dataset couldn't be processed using Pandas and Pyspark was a good way to go. Also, I used S3 to store the data model. It was a good choice as the aws service had a low cost and is designed for storing large volumes of data.
- How often the data should be updated and why?
    - The dim_cities_airports data should be updated at least once in 2 years, that's because the airports numbers will barely change in each city if they ever change.
    - The dim_cities_demographics data should be updated at least once a year, that's because we can notice a slight change in the stats from one year to another with regard to this data.
    - The fact_immigration data should be updated every month, that's because we can notice a high volume of data registered every month.
- How would I approach the problem differently under the following scenarios ?
    - The data was increased by 100x:
        - I managed to process this data using spark in a standalone mode, but if the data increased by 100x or more, I will have to switch to the cluster mode using EMR aws service for example.
    - The data populates a dashboard that must be updated on a daily basis by 7am every day.
        - In such a case, I will need a framework that schedules data pipeline executions on a daily basis. I will use Apache Airflow to create a DAG capable of managing these pipelines.
    - The database needed to be accessed by 100+ people.
        - If our database needs to be accessed by lots of people in a short period of time, then I will store the data in Redshift or BigQuery which are both designed to manage a data warehouse and are optimized for access and querying.