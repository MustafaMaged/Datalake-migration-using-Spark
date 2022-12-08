#import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType


# config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create spark session with hadoop package
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    loads song_data from S3, loads it to spark dataframe,
    process it, then writes the required tables in parquet format
    and sending them back to S3 
    
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    song_data = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_data.select('song_id', 'title', 'artist_id', 'year', 'duration') 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year',
                                  'artist_id').mode('overwrite').parquet(output_data+'data_parquet/song_table.parquet')


    # extract columns to create artists table
    artists_table = song_data.select('artist_id',
                                 'artist_name',
                                 'artist_location',
                                 'artist_latitude',
                                 'artist_longitude') 
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'data_parquet/artists_table.parquet')


def process_log_data(spark, input_data_log, input_data, output_data):
    """
    loads log_data from S3, loads it to spark dataframe,
    process it, then writes the required tables in parquet format
    and sending them back to S3 
    
    """
    # read log data file
    log_data = spark.read.json(input_data_log) 
    
    # filter by actions for song plays
    log_data = log_data.filter('page = "NextSong"') 

    # extract columns for users table    
    log_data.createOrReplaceTempView('log_table')
    users_table = spark.sql("""
    SELECT userId as user_id,
           firstName as first_name,
           lastName as last_name,
           gender,
           level
    FROM log_table

    """) 
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'data_parquet/users_table.parquet')


    # create timestamp column from original timestamp column
    # get_timestamp = udf(lambda x: to_timestamp(x/1000))
    log_data = log_data.withColumn('ts1', to_timestamp(log_data.ts/1000))
    
    # create datetime column from original timestamp column
    # get_datetime = udf(lambda x: to_date(to_timestamp(x/1000)))
    log_data = log_data.withColumn('ts_date', to_date(to_timestamp(log_data.ts/1000))) 
    
    # extract columns to create time table
    log_data.createOrReplaceTempView('log_table')
    time_table = spark.sql("""
                SELECT ts1 AS start_time,
                       EXTRACT(hour from ts_date) AS hour,
                       EXTRACT(day from ts_date) AS day,
                       EXTRACT(week from ts_date) AS week,
                       EXTRACT(month from ts_date) AS month,
                       EXTRACT(year from ts_date) AS year,
                       WEEKDAY(ts_date) AS weekday     
                FROM log_table
    """) 

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').\
    mode('overwrite').parquet(output_data+'data_parquet/time_table.parquet')


    # read in song data to use for songplays table
    # get filepath to song data file
    song_data = input_data_songs + 'song_data/A/A/A/*.json'
    
    # read song data file
    song_data = spark.read.json(song_data)
    song_data.createOrReplaceTempView('song_table')
    log_data.createOrReplaceTempView('log_table')
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                    SELECT  log.ts1 AS start_time,
                            log.userId AS user_id,
                            log.level,
                            s.song_id,
                            s.artist_id,
                            log.sessionId AS session_id,
                            log.location,
                            log.userAgent AS user_agent,
                            month(log.ts1) AS month,
                            year(log.ts1) AS year        
                    FROM song_table s
                    JOIN log_table log
                    ON log.artist=s.artist_name
                      AND log.song=s.title
    ''')  

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month')\
                   .mode('overwrite')\
                   .parquet(output_data+'data_parquet/songplays_table.parquet')


def main():
    spark = create_spark_session()
    input_data_songs = "s3a://udacity-dend/"
    input_data_log = "s3a://datalakemustafa/sparkify_log_small.json"
    output_data = "s3a://datalakemustafa"
    
    process_song_data(spark, input_data_songs, output_data)    
    process_log_data(spark, input_data_log, input_data_songs, output_data)


if __name__ == "__main__":
    main()
