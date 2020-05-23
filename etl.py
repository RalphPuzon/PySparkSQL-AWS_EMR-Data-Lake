import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():

	"""
	Creates a spark session instance object
	
	Arguments:
		None
		
	Returns:
		 A spark session instance object
	"""
	
    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):

	"""
	Reads the song_data JSON, and creates the songs and artists tables per specs. the tables are then
	written into the S3 bucket as parquet files.
	
	Arguments:
		spark:      The current spark session being used
		input_data: Directory path of where raw song_data exists. In this case it will be an S3 bucket
		input_data: Directory path of where processed data will be stored. In this case it will be an 
		            S3 bucket
		
	Returns:
		 None
	"""

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    song_df = spark.read.json(song_data) #should infer schema automatically
    
    # extract columns to create songs table
    songs_table = song_df.select("song_id",
                                 "title",
                                 "artist_id",
                                 "year",
                                 "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs.parquet", mode = "overwrite",
                              partitionBy = ["year", "artist_id"])

    # extract columns to create artists table
    artists_table = song_df.select("artist_id", 
                              col("artist_name").alias("name"), 
                              col("artist_location").alias("location"),
                              col("artist_latitude").alias("latitude"),
                              col("artist_longitude").alias("longitude")).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet", mode = "overwrite")

def process_log_data(spark, input_data, output_data):

	"""
	Reads the log_data JSON, and creates the songs and artists tables per specs. the tables are then
	written into the S3 bucket as parquet files. Finally, the fact table "songplays" is created by
	joining the songs table with the logs table.
	
	Arguments:
		spark:      The current spark session being used
		input_data: Directory path of where raw log_data exists. In this case it will be an S3 bucket
		input_data: Directory path of where processed data will be stored. In this case it will be an 
		            S3 bucket
		
	Returns:
		 None
	"""
	
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    logs_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    logs_df = logs_df.where(logs_df.page == "NextSong")

    # extract columns for users table    
    users_table = logs_df.select(col("userId").alias("user_id"),
                            col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"),
                            "gender",
                            "level").distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet", mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf (lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    logs_df = logs_df.withColumn("time_stamp", get_timestamp(logs_df.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())  
    logs_df = logs_df.withColumn("datetime", get_datetime("time_stamp"))
    
    # extract columns to create time table
    time_table = logs_df.select("time_stamp")\
                           .withColumn("hour", hour(logs_df.time_stamp))\
                           .withColumn("day", dayofmonth(logs_df.time_stamp))\
                           .withColumn("week", weekofyear(logs_df.time_stamp))\
                           .withColumn("month", month(logs_df.time_stamp))\
                           .withColumn("year", hour(logs_df.time_stamp))\
                           .withColumn("weekday", dayofweek(logs_df.time_stamp))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time.parquet", mode = "overwrite",
                             partitionBy = ["year", "month"])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = logs_df.withColumn("songplay_id", monotonically_increasing_id())\
                                .join(song_df, logs_df.song == song_df.title, how = 'left')\
                                . select("songplay_id",\
                                         col("time_stamp").alias("start_time"),\
                                         col("userId").alias("user_id"),\
                                         "level",\
                                         "song_id",\
                                         "artist_id",\
                                         col("sessionId").alias("session_id"),\
                                         "location",\
                                         col("userAgent").alias("user_agent"))\
    
    # write songplays table to parquet files partitioned by year and month
    # we cannot partition by year/month, since songplyas table does not contain year or month.
    songplays_table.write.parquet(output_data + "songplays.parquet", mode = "overwrite")

def main():
	"""
	A spark session is created, and used to read in the logs and songs data.
	the tables are then generated by spark and stored onto the S3 buckets.
	
	Arguments:
		None
		
	Returns:
		None
	"""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://dlake-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()