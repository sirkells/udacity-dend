# Project: Data Modelling with Postgres 

## 1. Context

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
The analytics team is particularly interested in understanding what songs users are listening to. 
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

My role is to create a database schema and ETL pipeline for this analysis. The Sparkify analytics team provided sample queries which can be used to test the database and ETL pipeline and compare the result with theirs.


## 2. Choice of Schema and ETL Pipeline

I chose a Star Schema design for the database because:

- Easy to design

- The Facts Table has four surrounding Dimension Tables (users, songs, artists and name) that are not elaborated; they dont have multiple level of relationships and child tables.


On using ETL Pipeline:
- To have a well designed structure and workflow for importing and transorming data 
- Automation

## Files used on the project

- *data*: Directory where all the json data are located

- *sql_queries.py*: This contains al sql queries needed.

- *create_tables.py*: This file contains imports queries from **sql_queries.py** file to drop and create tables. It drops all tables and recreates them everytime its run. This should be run before running the **etl.py** file.

- *test.ipynb*: This notbook is used to verify if all tables where created after running the **create_tables.py** file. It can also be used at the end to test if data was inserted.

- *etl.ipynb*: This notebook is used after running the **create_tables.py** file to create functions that read, transform and insert a single file from song_data and log_data into their respective tables. 

- *etl.py*: This file implements the functions created in the **etl.ipynb** file. Running this file reads, transforms and inserts rows into their respective tables.

- *README.md* You are currently reading me :). It contains all information and steps used in completing this project.


### Steps taken to complete project

- Wrote DROP, CREATE and INSERT queries statements in sql_queries.py

- Ran ```python create_tables.py``` file in console

- Verified if all tables were correctly created using **test.ipynb** 

- Using **etl.ipynb**, i created functions that read, transform and insert a single file from song_data and log_data into their respective tables 

- Verified if all rows were correctly transformed and inserted using **test.ipynb**

- Reset and drop all tables using **create_tables.py**

- Implemented the reads and transformation functions from **etl.ipynb** notebook in **etl.py**

- Ran ```python etl.py``` file in console to extract, transform and load data into the database.

- Verified if all rows were correctly extracted, transformed and inserted using **test.ipynb**

