# Building the Sparkify Song Plays Analysis Datalake

#### Purpose:
Client Sparkify has now grown a lot more, and is now requesting for an AWS hosted data lake with raw data stored in S3 buckets, where data can be queried via pyspark.

#### Overview of the Databases and Database Schema:

###### datalake:
- the datalake is an Amazon web services S3 bucket, containing a set of 5 folders, each representing the tables The final schema of the tables still share the 5 table star schema of the previous database:
- 
###### fact table: "songplays"
- The fact table supplies the metrics used for the song plays analytics.
    - columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

###### dimension table: "users"
- the registered users in the app.
    - columns: user_id, first_name, last_name, gender, level
    
###### dimension table: "songs" 
- the available songs in the database.
    - columns: song_id, title, artist_id, year, duration
    
###### dimension table: "artists"
- the available artists in the database.
    - columns: artist_id, name, location, latitude, longitude
    
###### dimension table: "time"
- a timestamp record of when a song was played.
    - columns: start_time, hour, day, week, month, year, weekday

#### Overview of the ETL Process:

Sparkify provided two sources of data coming from the "song_data" directory, and the "log_data" S3 buckets. The data is in JSON. fortunately, spark is able to handle JSON formatting and automatic schemainference easily.

*prior to running scripts, make sure the buckets and the Elastic MapReduce (EMR) cluster is running, and that user can connect to the cluster, either via SSH or PuTTY*.
    
- when the ETL script is ran, a spark instance is created, which reads the JSON files into a dataframe. the dataframes are then automtically cleaned, arranged and prepped for querying via SQL-like statements via spark. 









      
