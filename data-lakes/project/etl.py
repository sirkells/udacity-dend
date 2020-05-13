import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql import types as T
#from pyspark.sql.functions import udf, col
#from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
#from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as TimeStamp



def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")  \
        .getOrCreate()
    spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")

    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function loads and processes the songdata from given input_data filepath (S3 or Local)
    and extract song_table and artists_table then store parquet files in output filepath.

    Arguments:
        spark: Spark Session. 
        input_data: input data file path. 
        output_data: output data file path for dimensional tables.

    Returns:
        song_table: path containing saved song_table (dimension table) parquet file
        artists_table: path containing saved artists_table (dimension table) parquet file
    """
    print("Processing Song Data Files Started")
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # Create song data Schema Data types
    songSchema = T.StructType([
        T.StructField("song_id",T.StringType()),
        T.StructField("artist_id",T.StringType()),
        T.StructField("artist_latitude",T.DoubleType()),
        T.StructField("artist_location",T.StringType()),
        T.StructField("artist_longitude",T.DoubleType()),
        T.StructField("artist_name",T.StringType()),
        T.StructField("duration",T.DoubleType()),
        T.StructField("num_songs",T.IntegerType()),
        T.StructField("title",T.StringType()),
        T.StructField("year",T.IntegerType()),
    ])
    
    # read song data file
    songdata_df = spark.read.json(song_data, schema=songSchema)
    songdata_df.printSchema()
    print("Extracting and Creating songs_table..")
    
    # extract columns to create songs table
    songdata_df.createOrReplaceTempView("songs_table")
    songs_table = spark.sql("""
    SELECT song_id, title, artist_id, year, duration
    FROM songs_table
    ORDER BY song_id
    """)
    
    # Write DF to Spark parquet file (partitioned by year and artist_id)
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs_table/")
    print("songs_table Succesfully Extracted and Written to output..")
    
    
    print("Extracting and Creating artists_table..")
    # extract columns to create artists table
    songdata_df.createOrReplaceTempView("artists_table")
    artists_table = spark.sql("""
        SELECT  artist_id        AS artist_id, 
                artist_name      AS name, 
                artist_location  AS location, 
                artist_latitude  AS latitude, 
                artist_longitude AS longitude 
        FROM artists_table
        ORDER BY artist_id desc
    """)
    
    # Write DF to Spark parquet file
    artists_table.write.parquet(output_data + "artists_table/")
    print("artists_table Succesfully Extracted and Written to output..")
    
    
    return songs_table, artists_table


def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads and processes the logdata from given input_data filepath (S3 or Local)
    and extract users_table, time_table and songplays_table then store parquet files in output filepath.
        
        Arguments:
        spark: Spark Session. 
        input_data: input data file path. 
        output_data: output data file path for fact and dimensional tables.

    Returns:
        users_table: path containing saved users_table (dimension table) parquet file
        time_table: path containing saved time_table (dimension table) parquet file
        songplays_table: path containing saved songplays_table (Fact table) parquet file
            
    """
    print("Processing logs Data Files Started")
    
    # get filepath to log data file
    log_data = input_data + 'log-data/*.json'
    
    # Read log_data
    logdata_df = spark.read.json(log_data)
    
    # Filter by page="NextSong"
    logdata_df = logdata_df.filter(logdata_df.page == 'NextSong')
    
    print("Extracting and Creating users_table..")
    # extract columns to create users table
    logdata_df.createOrReplaceTempView("users_table")
    users_table = spark.sql("""
        SELECT  DISTINCT userId    AS user_id, 
                         firstName AS first_name, 
                         lastName  AS last_name, 
                         gender, 
                         level
        FROM users_table
        ORDER BY last_name
    """)
    
    # Write DF to Spark parquet file
    users_table.write.parquet(output_data + "users_table/")
    print("users_table Succesfully Extracted and Written to output..")
    
    # Create a new timestamp formatted column
    @udf(T.TimestampType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts / 1000.0)

    logdata_df = logdata_df.withColumn("timestamp", get_timestamp("ts"))
    
    # Create a new datetime column
    @udf(T.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

    logdata_df = logdata_df.withColumn("datetime", get_datetime("ts"))
    
    # extract columns to create time table
    print("Extracting and Creating time_table..")
    logdata_df.createOrReplaceTempView("time_table")
    time_table = spark.sql("""
        SELECT  DISTINCT datetime AS start_time, 
                         hour(timestamp) AS hour, 
                         day(timestamp)  AS day, 
                         weekofyear(timestamp) AS week,
                         month(timestamp) AS month,
                         year(timestamp) AS year,
                         dayofweek(timestamp) AS weekday
        FROM time_table
        ORDER BY start_time
    """)
    
    # Write time_table partitioned by year and month to Spark parquet file
    time_table.write.partitionBy("year", "month").parquet(output_data + "time_table/")
    print("time_table Succesfully Extracted and Written to output..")
    
    # Join songs and logdata using song and artist name
    print("Performing Joins")
    artists_songs_and_logs_df = logdata_df.join(songdata_df, (logdata_df.artist == songdata_df.artist_name) & (logdata_df.song == songdata_df.title))
    
    artists_songs_and_logs_df = artists_songs_and_logs_df.withColumn("songplay_id", F.monotonically_increasing_id())

    # extract columns to songplays table
    print("Extracting and Creating songplays_table..")
    artists_songs_and_logs_df.createOrReplaceTempView("songplays_table")
    songplays_table = spark.sql("""
        SELECT  songplay_id AS songplay_id, 
                timestamp   AS start_time, 
                userId      AS user_id, 
                level       AS level,
                song_id     AS song_id,
                artist_id   AS artist_id,
                sessionId   AS session_id,
                location    AS location,
                userAgent   AS user_agent
        FROM songplays_table
        ORDER BY (user_id, session_id) 
    """)
    
    # Write songplays_table partitioned by year and month to Spark parquet file
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays_table/')
    print("songplays_table Succesfully Extracted and Written to output..")
    
    
    
    return users_table, time_table, songplays_table
    
    
    

def main():
    """
    Description: This function performs the following:
            - gets S3 config details
            - creates a spark session
            - implements the ETL pipline processing stages defined in process_song_data() and process_log_data()

    Arguments:
        None

    Returns:
        None
    """
  
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    spark = create_spark_session()
    input_data = config['AWS']['INPUT_DATA']
    output_data = config['AWS']['OUTPUT_DATA']
    
    
    start = datetime.now()
    
    print("ETL Pipeline started".format(start))
    
    
    start_songs = datetime.now()
    songs_table, artists_table = process_song_data(spark, input_data, output_data)
    stop_songs = datetime.now()
    
    print("Succesfully processed song data files")
    print("Duration: {}".format(stop_songs-start_songs))
    
    
    start_logs = datetime.now()
    users_table, time_table, songplays_table = process_log_data(spark, input_data_ld, input_data_sd, output_data)
    stop_logs = datetime.now()
    
    print("All Logs Data files have been succesfully processed")
    print("Time: {}".format(stop_logs-start_logs))
    
    print("ETL pipeline completed.")
    stop = datetime.now()
    print("Duration: {}".format(stop-start))

if __name__ == "__main__":
    main()
