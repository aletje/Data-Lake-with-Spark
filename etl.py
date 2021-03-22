import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Description: 
        - Instantiates a SparkSession

        Returns:
        - Spark instance object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: 
        - Extract song meta data from JSON files stored in S3 bucket
        - Transforms song meta data into two separate DataFrames; songs_table and artists_table
        - Loads them back into s3 as parquet files stored in a separate s3-bucket for analytical purposes

        Arguments:
        - Parameter spark: the instantiated SparkSession
        - Parameter input_data: input path
        - Parameter output_data: output path

        Returns:
        - None
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.selectExpr(["song_id", "title", "artist_id", "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id"                   ,\
                                   "artist_name as name"         ,\
                                   "artist_location as location" ,\
                                   "artist_latitude as latitude" ,\
                                   "artist_longitude as longitude"]).dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
        Description: 
        - Extract log data from JSON files stored in S3 bucket
        - Transforms log data into three separate DataFrames; users_table, time_table and songplays_table
        - Loads them back into s3 as parquet files stored in a separate s3-bucket for analytical purposes

        Arguments:
        - Parameter spark: the instantiated SparkSession
        - Parameter input_data: input path
        - Parameter output_data: output path

        Returns:
        - None
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df["page"] == "NextSong")

    # extract columns for users table    
    users_table = df.selectExpr(["userId as user_id"       ,\
                                 "firstName as first_name" ,\
                                 "lastName as last_name"   ,\
                                 "gender"                  ,\
                                 "level"]).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda epoch_time: datetime.fromtimestamp(epoch_time / 1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda epoch_time: datetime.fromtimestamp(epoch_time / 1000), DateType())
    df = df.withColumn('datetime', get_datetime(col('ts')))
    
    # extract columns to create time table
    time_table = df.select([hour("timestamp").alias("hour")       ,\
                            dayofmonth("timestamp").alias("day")  ,\
                            weekofyear("timestamp").alias("week") ,\
                            month("timestamp").alias("month")     ,\
                            year("timestamp").alias("year")       ,\
                            date_format("timestamp", 'E').alias("weekday")]).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df, df.song == song_df.title)\
    .selectExpr(["timestamp as start_time"  ,\
                 "userid as user_id"        ,\
                 "level"                    ,\
                 "song_id"                  ,\
                 "artist_id"                ,\
                 "sessionid as session_id"  ,\
                 "location"                 ,\
                 "useragent as user_agent"]) \
    .withColumn("year", year("start_time"))  \
    .withColumn("month", month("start_time")).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.createOrReplaceTempView("songplays")
    spark.sql("""
            SELECT row_number() over (order by start_time asc) as songplay_id,
                   start_time,
                   user_id,
                   level,
                   song_id,
                   artist_id,
                   session_id,
                   location,
                   user_agent,
                   year,
                   month
            FROM songplays
    """).write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays.parquet")


def main():
    """
        Description:
        - Creates spark session
        - Creates i/o path variables
        - Calls the process_song_data function
        - Calls the process_log_data function

        Arguments:
        - Parameter spark: the instantiated SparkSession
        - Parameter input_data: input path
        - Parameter output_data: output path

        Returns:
        - None
    """
    spark = create_spark_session()
    input_data  = "s3a://udacity-dend/"
    output_data = "s3a://aletje/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    

if __name__ == "__main__":
    main()
