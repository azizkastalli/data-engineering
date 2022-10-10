import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import DateType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates and returns a spark session.
    
    args:
        None
        
    returns:
        spark (SparkSession): a SparkSession object used to run spark tasks on a distributed cluster.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    This funciton extracts the songs data from an S3 bucket, creates the songs and artists
    dataframes and save them into another S3 bucket in parquet format.
    
    args:
        spark (SparkSession): 
            A SparkSession object used to run spark tasks on the cluster.
        input_data (str):
            An str object representing the input data path
        output_data (str): 
            An str object representing the output data path
    returns:
        None.
    """
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select([
        'song_id', 'title', 'artist_id', 
        'year', 'duration'
    ]).dropDuplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id')\
                     .mode('overwrite')\
                     .parquet(output_data+'songs.parquet')

    # extract columns to create artists table
    artists_table = df.select([
        col('artist_id'), 
        col('artist_name').alias('name'), 
        col('artist_location').alias('location'), 
        col('artist_latitude').alias('latitude'), 
        col('artist_longitude').alias('longitude')
    ]).dropDuplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite')\
                       .parquet(output_data+'artists.parquet')

def process_log_data(spark, input_data, output_data):
    """
    This funciton extracts the log data from an S3 bucket, creates the users, time
    and songplays dataframes and save them into another S3 bucket in parquet format.
    
    args:
        spark (SparkSession): 
            A SparkSession object used to run spark tasks on the cluster.
        input_data (str):
            An str object representing the input data path
        output_data (str): 
            An str object representing the output data path
    returns:
        None.
    """
    
    # get filepath to log data file
    log_data = input_data+'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    artists_table = df.select([
        col('userId').alias('user_id'), 
        col('firstname').alias('first_name'), 
        col('lastname').alias('last_name'), 
        col('gender'), 
        col('level')
    ]).dropDuplicates(subset=['user_id'])
    
    # write users table to parquet files
    artists_table.write.mode('overwrite').parquet('users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x / 1e3), TimestampType())
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromtimestamp(x / 1e3), DateType())
    df = df.withColumn('datetime', get_timestamp('ts'))
    
    # extract columns to create time table
    time_table = df.select([
        col('ts').alias('start_time'),
        hour(col('datetime')).alias('hour'), 
        dayofmonth(col('datetime')).alias('day'),
        weekofyear(col('datetime')).alias('week'), 
        month(col('datetime')).alias('month'), 
        year(col('datetime')).alias('year'),
        dayofweek(col('datetime')).alias('weekday')
    ]).dropDuplicates(subset=['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month')\
                    .mode('overwrite')\
                    .parquet('time.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs.parquet')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(
        song_df, 
        on=[
            df.song == song_df.song_id, 
            df.artist == song_df.artist_id
        ],
        how='left'
    ).join(
        time_table,
        on=[df.ts == time_table.start_time],
        how='left'
    ).select([
        monotonically_increasing_id().alias('songplay_id'), 
        'start_time', 
        col('userId').alias('user_id'),
        'level',
        'song_id', 
        'artist_id',
        col('sessionId').alias('session_id'), 
        'location',
        col('userAgent').alias('user_agent'), 
        'month',
        time_table.year
    ])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month')\
                    .mode('overwrite')\
                    .parquet('songplays.parquet')


def main():
    """
    main execution function.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()