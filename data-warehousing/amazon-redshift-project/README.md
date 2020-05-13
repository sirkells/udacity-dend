# Project: Data Warehouse

## 1. Context

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

My role is to to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

The Sparkify analytics team provided sample queries which can be used to test the database and ETL pipeline and compare the result with theirs.



## 2. Choice of Schema and ETL Pipeline

I chose a Star Schema design for the database because:

- Easy to design

- The schema design is suitable for Analytics purposes

- Contains two staging tables (staging_events and staging_songs)

- The Facts Table called songplays contains records in event data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent 
    
- The Schema has four Dimension Tables (users, songs, artists and name) that are not elaborated; they dont have multiple level of relationships and child tables.


On using ETL Pipeline:
- To have a well designed structure and workflow for importing and transorming data 
- Automation

## Files used on the project

- *sql_queries.py*: This contains al sql queries needed.

- *create_tables.py*: This file contains imports queries from **sql_queries.py** file to drop and create tables. It drops all tables and recreates them everytime its run. This should be run before running the **etl.py** file.

- *etl.py*: is used after running the **create_tables.py** file to create functions transform and insert data from S3 to staging tables and from staging tables to analytics (dimensional) tables on Redshift using queries written in sql_queries script.

- *README.md* You are currently reading me :). It contains all information and steps used in completing this project.

- *create_redshift_cluster.py*: creates aws redshift cluster

## How to run

- Create a config file in the root directory called dwh.cfg with your AWS, DWH, CLUSTER, IAM_ROLE and S3 Buckets details like below
```
[AWS]
KEY=
SECRET=

[DWH]
DWH_CLUSTER_TYPE       = multi-node
DWH_NUM_NODES          = 4
DWH_NODE_TYPE          = dc2.large
DWH_CLUSTER_IDENTIFIER = 
DWH_DB                 = 
DWH_DB_USER            = 
DWH_DB_PASSWORD        = 
DWH_PORT               = 5439
DWH_IAM_ROLE_NAME      = 

[CLUSTER]
HOST=''
DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=5439

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```


- Install dependencies

    `$ pip install -r requirements.txt` 
    
- Set up Amazon Redshift Cluster, S3 Buckets, EC2 Instance and IAM Role by running *create_redshift_cluster* file

- Run the *create_tables* script to set up the database staging and analytical tables

- Run the *etl* script to insert data from S3 to staging tables and from staging tables to analytics (dimensional) tables on Redshift

    `$ python create_tables.py`

## Steps taken to complete project

### Create Table Schemas

1. Design schemas for your fact and dimension tables
2. Write a SQL CREATE statement for each of these tables in sql_queries.py
3. Complete the logic in create_tables.py to connect to the database and create these tables
4. Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you   want to reset your database and test your ETL pipeline.
5. Launch a redshift cluster and create an IAM role that has read access to S3.
6. Add redshift database and IAM role info to dwh.cfg.
7. Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.

### Build ETL Pipeline

1. Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
2. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
3. Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
4. Delete redshift cluster when finished.


