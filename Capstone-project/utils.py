from pyspark.sql.functions import udf, expr, col, to_date, lower, upper, count, when, isnan, lit, round
from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, DateType
from pyspark.sql import SparkSession
import re
import pandas as pd


def sparkContext():
    """
    This function creates and returns a spark session.
    
    args:
        None
        
    returns:
        spark (SparkSession): a SparkSession object used to run spark tasks on a distributed cluster.
    """
    
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    return spark


def dataExplorationCleanining(spark_data, data_label, duplicates_fields=None, show=False, nans_threshold=0.8):
    """
    A function that performs a basic data exploration and cleaning of a spark dataframe.
    The cleaning process consists of droping rows with only nans values and duplicates.
    Also, the funciton drops some features with nans above a certain threshold "nans_threshold".
    ----
    
    Args:
        spark_data(Spark.DataFrame): a spark dataframe.
        data_label(string): the spark dataframe label.
        duplicates_fields(list): a list of features used to detected duplicated rows.
        show(boolean): if set to True, it will show the cleaning results, otherwise the function will run on silent mode.
        nans_threshold(float): a threshold used to determine when a column must be dropped based on its nans percentage.
        
    Returns:
        spark_data(Spark.DataFrame): a spark dataframe.
    """
    
    original_count = spark_data.count()
    
    #drop duplicates
    spark_data = spark_data.dropDuplicates(duplicates_fields)
    
    #drop all nans rows
    spark_data = spark_data.dropna(how='all')

    #drop columns with nans values more than a threshold
    percent_missing = spark_data.select([
        round((count(when(isnan(c) | col(c).isNull(), c))/count(lit(1))), 4).alias(c) 
        for c in spark_data.columns
    ])
    precent_missing_list = percent_missing.collect()
    if len(precent_missing_list) > 0:
        drop_features = [c for c in spark_data.columns if precent_missing_list[0][c]>nans_threshold]
        spark_data = spark_data.drop(*drop_features)
    else:
        drop_features = []

    if show:
        print(
            f"""\t\t\t\t\t\t\t\t\t*********** {data_label.upper()} Basic Data Exploration and Cleaning ***********\n"""
        )
        print(f"""----------- {data_label.upper()} Top 5 Rows --------------\n""")
        spark_data.show(5)
        print(f"""----------- {data_label.upper()} Data Schema --------------\n""")
        spark_data.printSchema()
        print(f"""---------- Data Shape For {data_label.upper()} ----------
        {data_label.upper()} Data Shape : Rows={spark_data.count()} Columns={len(spark_data.columns)}\n""")    
        print(f"""---------- Data Duplicates For {data_label.upper()} ----------
        {data_label.upper()} Duplicated values = {
            original_count - spark_data.count()
        }\n""")
        print(f"---------- Missing Data Percentage For {data_label.upper()} ----------")
        percent_missing.show()
        print(f""" Dropped features : {drop_features}""")
    
    return spark_data


def map_features_extraction(file):
    """
    Extracts fields and their corresponding values in form of a dictionary
    the fields, values are extracted from a .SAS file.
    ---
    
    args:
        file(string): file label used to extract the fields, values.
    
    returns:
        all_fields_maps(dictionary): a dicationary containing fields and their corresponding values.
    """
    
    ####
    with open(file, 'r') as file:
        lines = file.readlines()
    
    ####
    fields_map = {}
    flag_search = False
    for i, line in enumerate(lines):
        if 'value' in line:
            key =  re.sub("[\t\n;'$]", '', line.split('value')[-1]).strip()  
            fields_map[key] = [i+1]
            flag_search = True
        if ';' in line and flag_search:
            fields_map[key].append(i+1)
            flag_search = False
            
            
    ####
    all_fields_maps = {}

    for key, value in fields_map.items():
        if len(value)<2:
            continue

        all_fields_maps[key] = {}
        for i in range(value[0], value[1]):
            try:
                field_key, field_value = lines[i].split('=')
            except Exception as e:
                print(e,' value:',lines[i])
            try:
                field_key = str(float(field_key))
            except:
                field_key =  re.sub("[\t\n;']", '', field_key).strip()

            field_value = re.sub("[\n;']", '', field_value).strip()
            all_fields_maps[key][field_key] = field_value
            
    return all_fields_maps


@udf(StringType())
def IsoRegionFormatCheck(iso_region):
    """
    Checks and rectifies US iso regions.
    ---
    
    args:
        iso_region(string): a string indicating a US iso region.
    
    returns:
        iso_region(string): a correct format of US iso region.
    """
    iso_region = iso_region.upper()
    iso_region = re.sub('US-', '', iso_region).strip()
    iso_region = re.sub(r'[^A-Za-z]', "", iso_region).strip()
    iso_region = 'US-'+iso_region[:2]
    return iso_region


@udf(StringType())
def extract_city(i94port):
    """
    Extracts city from i94 port feature (immigration dataset)
    """
    return i94port.split(',')[0].lower()


@udf(DateType())
def convertSaS_toDate(sas_date):
    """
    Converts a SAS date format to a pandas date format.
    """
    return (
        pd.to_timedelta(sas_date, unit='D') + pd.Timestamp('1960-1-1')
    ).date()


def unique_key_check(spark_data, spark_data_label, key):
    """
    Checks if a key is unique or not by printing a message.
    ---
    
    args:
        spark_data(Spark.DataFrame): spark dataframe representing a fact or dimension table.
        spark_data_label(string): the dataframe label.
        key(string): the unique key field.
    
    returns:
        None.
    """
    
    if spark_data.count() - spark_data.select(key).distinct().count() == 0:
        print(f'{spark_data_label}: unique primary key test passed')
    else:
        print(f'{spark_data_label}: unique primary key test failed!')
        
        
def check_missing_values(spark_data, spark_data_label, subset):
    """
    Counts the missing values of a spark data frame primary/foreign keys
    and prints a message to indicate if the test passed or not.
    ---
    
    args:
        spark_data(Spark.DataFrame): spark dataframe representing a fact or dimension table.
        spark_data_label(string): the dataframe label.
        subset(list): a list of one or multiple column names.
    
    returns:
        None.
    """
    print(f'{spark_data_label} keys missing values test passed.'
    if spark_data.select([
        (count(when(col(c).isNull(), c))/count(lit(1))).alias(c) 
        for c in subset
    ]).toPandas().sum().sum() == 0 else 
    f'{spark_data_label}: keys missing values test failed!'
    )