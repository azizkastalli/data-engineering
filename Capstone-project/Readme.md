# Project Title
### Data Engineering Capstone Project

#### Project Summary

The project consists of creating a data model following a star schema that contains data about immigrants moving to/from US cities. The data model will contain not only information about immigrants but also information about US cities demographics as well as their airports.<br>
The main challenge of this project is to clean and transform data from different sources to generate a data model that represents a unique ground truth about the data and that will be used for analytical processes.<br>
Once the data model is ready for processing, we can get insights such as what cities are prefered by immigrants and we can even filter by age, race and many other dimensions. We can also check if the available airports in the cities can handle the flow of immigrants.<br>
<br>
The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up
<br>

### Step 1: Scope the Project and Gather Data

#### Scope

During the next steps, I will read the data and check if there are any anomalies such as missing data and duplicates.<br> Next, I will define a data model following a star schema, then, I will run data pipelines to aggregate the data by cities and generate dimension and fact tables.<br> Finally, I will check the quality of the data and write the tables as parquet files on S3 which is going to be the final form or output of this project. During these steps I will use pandas and pyspark for data processing. pyspark will be extremely helpful especially when processing the immigration dataset which contains over 3 million rows/samples.

#### Data Description

Our data is derived from 3 sources or files as follows:
* [I94 Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html): This data comes from the US National Tourism and Trade Office. It contains detailed information about visitors arrival/departure to/from US states during the period of april 2016.
* [U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/): This data comes from OpenSoft. It contains detailed demographic statistics for each state and city in the US.
* [Airport Code Table](https://datahub.io/core/airport-codes#data): This data contains table of airport codes and corresponding cities.


### Step 2: Explore and Assess the Data

#### Cleaning Steps

Steps necessary to clean the data

1. check and drop duplicated data
2. drop features with nans over 80%
3. drop rows with only nans
4. check statistical anomalies and fields format

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model

![image](https://drive.google.com/uc?export=view&id=1krP58ZqA2qFRXoXVvoxK-s8Cc18YBDO7)

The data model is a star schema model. It consists of 3 dimension tables:<br>
- dim_cities_airport: contains airport information aggregated by cities.
- dim_cities_demographics: contains statistical data about cities demographics and aggregated by cities.
- dim_time: contains features extracted from the immigration dates such as year, month, weekday etc.<br>
As well as a fact table (fact_immigration) which contains information about each immigrant.<br>

The star schema model was the preferred choice as we can get analytical insights using a few joins.<br>
Also, the aggregation by cities of both dim_cities_airport and dim_cities_demographics dimension tables was a good choice for this model, as it is the only possible way to link the immigration data with both dimensions (airports and cities demographics).<br>
Aggregating airports by cities made it possible to link each row from the fact table (extracted from the immigration dataset) with each row from both dim_cities_airport and dim_cities_demographics dimension tables.<br>

#### 3.2 Mapping Out Data Pipelines
The steps necessary to pipeline the data into the chosen data model are as follows:
- Generate the cities airport dimension table (<b>dim_cities_airports</b>):
    1. Check and rectify the iso_region column format and filter the airports to keep only U.S airports.
    2. Drop any nan value in the "municipality" feature and transform it to lower case.
    3. Select airport fields and rename them, generate primary key.
    4. Aggregate airports by cities.
    5. Create the dim_cities_airport view
<br>

- Generate the cities demographics dimension table (<b>dim_cities_demographics</b>):
    1. Generating race features and aggregate them by cities.
    2. generating primary key composed of cities and state_codes.
    3. rename columns
    4. create the dim_cities_demographics view
<br>
 
- Generate the time dimension table (<b>dim_time</b>):
    1. Drop nans from arrival and departure dates (from immigration dataset) and convert them from SAS to date format.
    2. Extracts features suchs as month, weekofyear, isweekend ...etc. from both arrival and departure date.
    3. Create the dim_time view.
<br>

- Generate the immigration fact table (<b>fact_immigration</b>):
    1. Extract a map of (field --> value) from the I94_SAS_Labels_Descriptions.SAS file and decode columns of the immigration dataset.
    2. Extracts the city name from the 'i94port' column after decoding in step1.
    3. Drop nans from arrival and departure dates (from immigration dataset) and convert them from SAS to date format.
    4. Select a set of columns to be included in the fact table.
    5. Set all city values to lower case and all state_code values to upper case.
    6. Create the fact_immigration view and join it with the time, cities_dimographics and cities_airports
<br>

#### 4.3 Data dictionary 

<b> dim_cities_demographics: </b><br>
* <b>dim_cities_demographics_pk:</b> Dimension table pripary key composed of state_code + city label.
* <b>city:</b> City label.
* <b>state:</b> State label.
* <b>state_code:</b> 2 character unique state code.
* <b>median_age:</b> City population median age.
* <b>male_population:</b> Total number of the city male population.
* <b>female_population:</b> Total number of the city female population.
* <b>total_population:</b> Total number of city population.
* <b>number_of_veterans:</b> Total number of city veterans.
* <b>foreign_born:</b> Total number of city foreign borns.
* <b>average_household_size:</b> Average of persons per household per city.
* <b>hispanic_or_latino_population:</b> Total number of hispanic or latino race population per city.
* <b>white_population:</b> Total number of white race population per city.
* <b>asian_population:</b> Total number of asian race population per city.
* <b>black_or_african_american_population:</b> Total number of black or african american race population per city.
* <b>american_indian_and_alaska_native_population:</b> Total number of american indian and alaska native race population per city.

<b> dim_cities_airports: </b><br>
* <b>dim_airport_pk:</b> Dimension table pripary key composed of state_code + city label.
* <b>city:</b> City label.
* <b>state_code:</b> 2 character unique state code.
* <b>total_large_airport:</b> Total number of large airports per city.
* <b>total_medium_airport:</b> Total number of medium airports per city.
* <b>total_small_airport:</b> Total number of small airports per city.
* <b>total_balloonport:</b> Total number of balloon port pere city.
* <b>total_seaplane_base:</b> Total number of seaplane bases per city.
* <b>total_heliport:</b> Total number of heliport per city.
* <b>total_airports:</b> Total number of active airports per city.
* <b>total_closed:</b> Total number of closed airports per city.
* <b>avg_elevation_ft:</b> Average of airports/bases elevation feat per city.

<b> dim_time: </b><br>
* <b>arrival_date_pk:</b> Dimension table pripary key representing arrival and departure dates.
* <b>year:</b> Date year, extracted from the arrival/departure date.
* <b>month:</b> Month of the year, extracted from the arrival/departure date.
* <b>day:</b> Day of month, extracted from the arrival/departure date.
* <b>weekday:</b> Day of week, extracted from the arrival/departure date.
* <b>weeofyear:</b> indicates the week year number.
* <b>season:</b> The season of the year, extracted from the arrival/departure date.
* <b>isweekend:</b> indicates whether a day is a week-end or not.

<b> fact_immigration: </b><br>
* <b>cicd_pk:</b> immigration fact primary key, indicates whether a day is a week-end or not.
* <b>arrival_date_fk:</b> immigration fact foreign key, links the fact table with the time dimension.
* <b>dim_airport_fk:</b>  immigration fact foreign key, links the fact table with the cities_airports dimension.
* <b>dim_cities_demographics_fk:</b>  immigration fact foreign key, links the fact table with the cities_demographics dimension.
* <b>departure_date:</b> the departure date from the US.
* <b>i94res:</b> immigrant residence country.
* <b>i94cit:</b> immigrant birth country.
* <b>biryear:</b> 4 digit year of birth.
* <b>gender:</b> non-immigrant sex.
* <b>airline:</b> airline used to arrive in U.S.
* <b>fltno:</b> the flight number.
* <b>i94visa:</b> visa codes indicating the reason for the immigration/travel.
* <b>i94mode:</b> indicates the means of transport used to move to/from the U.S.
* <b>visatype:</b> class of admission legally admitting the non-immigrant to temporarily stay in U.S.
<br>

#### Step 5: Complete Project Write Up
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
        - If our database needs to be accessed by lots of people and in a short period of time, then I will store the data on Redshift or BigQuery which are both designed to manage a data warehouse and are optimized for access and querying.


