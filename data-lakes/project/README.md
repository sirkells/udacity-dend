# Project: Data Lake

## 1. Context

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

My role as their data engineer is to to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 

The goal is to enable their analytics team in finding insights in what songs their users are listening to.

The Sparkify analytics team provided sample queries which can be used to test the database and ETL pipeline and compare the result with theirs.



## 2. Choice of Schema and ETL Pipeline

I chose a Star Schema design for the database because:

- Easy to design

- The schema design is suitable for Analytics purposes

- The Facts Table called songplays contains records in log data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent 
    
- The Schema has four Dimension Tables (users, songs, artists and name) that are not elaborated; they dont have multiple level of relationships and child tables.


On using ETL Pipeline:
- To have a well designed structure and workflow for importing and transorming data 
- Automation

## Files used on the project

- *etl.py*: reads data from S3, processes that data using Spark, and writes the tables created in parquet format back to S3.

- *README.md* You are currently reading me :). It contains all information and steps used in completing this project.

- *dl.cfg*: contains your AWS credentials

## How to run

- Create a config file in the root directory called dwh.cfg with your AWS and S3 Buckets details like below
```
[AWS]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
INPUT_DATA=s3a://datalake-u/data/
OUTPUT_DATA=s3a://datalake-u-output/data/
```
- run etl.py script

## Build ETL Pipeline

1. etl.py to load song and log data from s3 

2. Spark processes and transforms data into one fact table (songplays partitioned by year and month) and four Dimension Tables; users, songs(partitioned by year and then artist), artists and name

3. Loads and writes processed tables in parquet form into S3 where the analytics team can further find insights.



