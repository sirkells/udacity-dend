import pandas as pd
import pycountry
from datetime import datetime, timedelta
import os, glob
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import udf, col, monotonically_increasing_id

root = ["../../", "/data/18-83510-I94-Data-2016/"]
source_cities_dem = 'source_data/us-cities-demographics.csv'
source_country = 'source_data/countries.csv'
source_country_mapping = 'source_data/country-mapping.txt'
source_airport = 'source_data/airport.txt'


staging_immigration_path = 'staging_files/immigration-data/'
staging_airport_path = 'staging_files/airport_data/'
staging_cities_dem_path = 'staging_files/us-cities-demograpy/'
staging_country_path = 'staging_files/country_data/'


airport_output_path = 'tables/airport/'
country_output_path = 'tables/country/'
passenger_output_path = 'tables/passenger/'
time_output_path = 'tables/time/'
entry_output_path = 'tables/entry/'


def create_spark():
    """
     Create or retrieve a Spark Session
    """

    spark = SparkSession.builder.config("spark.jars.packages",
                                        "saurfang:spark-sas7bdat:2.0.0-s_2.11").enableHiveSupport().getOrCreate()
    return spark


def get_files(root_path):
    """
     Get data files directory
     """

    folder_name = root_path[1]
    path = "".join(root_path)
    data_folders = os.walk(path)
    files_dir = [folder_name + file for folder in data_folders for file in folder[2]]
    return files_dir


def map_country(path):
    """
        Maps countries name and code from i94 labels descriptions
    """
    countries = dict()
    with open(path) as f:
        for line in f.readlines():
            s, b = line.split(' = ')
            b = b.replace("'", "").strip()
            countries[int(s)] = b.capitalize()
    return countries


def get_name(x):
    """
        Description: This function extracts more country details using the pycountry module

        Arguments:
            x: country name

        Returns:
            data: list() containing country's alpha_2, alpha_3, official_name, official_code
        """
    try:
        if x == "Samoa" or x == "Niger" or x == "GUINEA" or x == "Guadeloupe" or x == "Mayotte":
            row = pycountry.countries.get(name=x)

        elif x == "Curacao":
            x = "Curaçao"
            row = pycountry.countries.get(name=x)
        else:
            row = pycountry.countries.search_fuzzy(x)[0]

        alpha_2, alpha_3, official_name, official_code = row.alpha_2, row.alpha_3, row.official_name, row.numeric
    except Exception:
        alpha_2, alpha_3, official_name, official_code = None, None, None, None
    data = [alpha_2, alpha_3, official_name, official_code]
    return data


def is_empty(df):
    """
    Description: This function checks if a spark data frame table is empty

    Arguments:
        df: spark data frame

    Returns:
        True or False
    """
    return df.rdd.isEmpty()


def has_null(df, query):
    """
    Description: This function checks if a spark data frame table column has a null value

    Arguments:
        df: spark data frame
        query: spark sql query

    Returns:
         True or False
    """
    return False if df.filter(query).rdd.isEmpty() else True


def data_type(df, dtype, col_name):
    """
    Description: This function checks the data type of columns

    Arguments:
        df: spark data frame
        dtype: data type
        col_name: column names

    Returns:
         True or False
    """
    return dict(df.dtypes)[col_name] != dtype


def data_quality_check(df, metric: dict, table):
    """
    Description: This function checks if spark df is empty, null and has wrong data types

    Arguments:
        df: spark data frame
        metric: dict containing:
                query: sql query to check if df is null
                dtype1, dtype2: expected data types in columns (name1, name2)
                name1, name2: columns names

    Returns:
         status: dict => msg: Status, code: 1 or 0
    """
    table_is_empty = is_empty(df)
    print(f"Passed: {table} Table is Not Empty")
    table_has_null = has_null(df, metric["query"])
    print(f"Passed: {table} Table has No Null Values")
    col1_has_wrong_data_type = data_type(df, metric['dtype1'], metric["name1"])
    print(f"Passed: Data type for {metric['name1']} column is correct")
    col2_has_wrong_data_type = data_type(df, metric['dtype2'], metric["name2"])
    print(f"Passed: Data type for {metric['name2']} column is correct")
    results = {table_is_empty, table_has_null, col1_has_wrong_data_type, col2_has_wrong_data_type}

    #status = "Failed" if 1 in results else "Passed"
    if 1 in results:
        status = {
            "msg": "Failed",
            "Code": 1
        }
    else:
        status = {
            "msg": "Passed",
            "Code": 0
        }

    if table_is_empty:
        print("Err: Table is Empty")
    elif table_has_null:
        print("Err: Table has Null")
    elif col1_has_wrong_data_type:
        print(f"Err: {metrics['name1']} should be {metrics['dtype1']} type")
    elif col2_has_wrong_data_type:
        print(f"Err: {metrics['name2']} should be {metrics['dtype2']} type")

    return status


def metrics(col: list, dtype: list):
    """
    Description: This function generates the metrics used in data_quality_check() function

    Arguments:
        col: list of metrics
        dtype: data type to ensure df column is

    Returns:
         metric: dict() =>      query: sql query
                                dtype1: str or int or timestamp
                                dtype2: str or int or timestamp
                                name1: column name 1
                                name2: column name 2
    """
    metric = dict()
    metric["query"] = f"{col[0]} is null or {col[1]} is null"
    metric["dtype1"] = f"{dtype[0]}"
    metric["dtype2"] = f"{dtype[1]}"
    metric["name1"] = f"{col[0]}"
    metric["name2"] = f"{col[1]}"
    return metric

# Spark User defined function
countries = map_country(source_country_mapping)
@udf(TimestampType())
def get_timestamp(arrdate):
    arrdate_int = int(arrdate)
    return datetime(1960, 1, 1) + timedelta(days=arrdate_int)

@udf(StringType())
def get_country(code):
    code = int(code)
    print(code)
    return countries[code] if code in countries else "Others"


def staging_immigration_data(spark, file_path, target, filename_by_number=False):
    """
    Description: This function can be used to read and copy the immigraion files from the source file_path,
    loads it into the staging directory with each directory representing each month (jan-dec)

    Arguments:
        spark: spark session
        file_path: input immigration data file path.
        target: destination directory
        filename_by_number: save directory name in number format

    Returns:
        data: spark df
    """

    filename = str(file_path.split('/')[-1].split('_')[1][:3])
    print(f"Staging Started => {filename}")
    if filename_by_number:
        filename = str(months.get(filename))
        filename = "0" + filename if len(filename) == 1 else filename

    output = target + filename

    default_missing_values = {
        'i94mode': 0.0,
        'i94addr': 'NA',
        'depdate': 0.0,
        'i94bir': 'NA',
        'i94visa': 0.0,
        'count': 0.0,
        'dtadfile': 'NA',
        'visapost': 'NA',
        'occup': 'NA',
        'entdepa': 'NA',
        'entdepd': 'NA',
        'entdepu': 'NA',
        'matflag': 'NA',
        'biryear': 0.0,
        'dtaddto': 'NA',
        'gender': 'NA',
        'insnum': 'NA',
        'airline': 'NA',
        'admnum': 0.0,
        'fltno': 'NA',
        'visatype': 'NA'
    }



    data = spark.read.format('com.github.saurfang.sas.spark').load(file_path)
    data = data.na.fill(default_missing_values)
    data.write.mode("overwrite").parquet(output)
    data = spark.read.parquet(output)
    print(f"Staging Completed => {filename}")
    return data


def staging_airport_data(spark, file_path, target):
    """
    Description: This function can be used to read, clean and copy the airport data file_path and
    loads it into the staging directory

    Arguments:
        spark: spark session
        file_path: input data file path.
        target: destination directory

    Returns:
        data: spark df
    """
    print("Staging Started")
    name_arr, code_arr, location_arr = list(), list(), list()

    # read airport data
    with open(file_path) as f:
        for line in f.readlines():

            # remove punctuations and white spaces
            code, name = line.split('=')
            code = code.strip().strip().replace("'", "")
            name = " ".join(name.strip().replace("'", "").split()).split(',')
            code_arr.append(code)

            if len(name) == 2:
                name1 = name[0]
                loc = name[1].strip()
            else:
                loc = code
                name1 = code

            name_arr.append(name1)
            location_arr.append(loc)

    data = list(zip(code_arr, name_arr, location_arr))

    df_airport = pd.DataFrame(data, columns=['airport_id', 'airport_name', 'airport_location'])
    df_airport['airport_id'] = df_airport['airport_id'].astype(str).str.replace('\[|\]|\'', '')
    df_airport['airport_name'] = df_airport['airport_name'].astype(str).str.replace('\[|\]|\'', '')
    df_airport['airport_location'] = df_airport['airport_location'].astype(str).str.replace('\[|\]|\'', '')
    # df_airport.head(10)

    # save csv
    df_airport.to_csv(target + 'csv/airport.csv', index=False)

    output = target + 'parquet/'
    print("Spark Processing")
    schema = StructType([
        StructField("airport_id", StringType(), False),
        StructField("airport_name", StringType(), False),
        StructField("airport_location", StringType(), False)
    ])
    df_spark = spark.createDataFrame(df_airport, schema=schema)

    df_spark.write.mode("overwrite").parquet(output)
    df_spark = spark.read.parquet(output)
    df_spark.show(5)
    print(f"Staging Completed ")
    return df_spark

def staging_cities_demo_data(spark, file_path, target):
    """
    Description: This function can be used to read and copy the city demographic data file_path and
    loads it into the staging directory

    Arguments:
        spark: spark session
        file_path: input data file path.
        target: destination directory

    Returns:
        data: staged data stored as parquet files
    """
    print("Staging Started")
    df = pd.read_csv(file_path, sep=';')
    df.fillna(0, inplace=True)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.lower()
    print(df.columns)

    df.to_csv(target + 'csv/cities-demo.csv', index=False)

    # data = spark.read.csv('airport-codes_csv.csv', header=True)
    output = target + 'parquet/'
    print("Spark Processing started")
    schema = StructType([
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("median_age", FloatType(), False),
        StructField("male_population", FloatType(), False),
        StructField("female_population", FloatType(), False),
        StructField("total_population", IntegerType(), False),
        StructField("number_of_veterans", FloatType(), False),
        StructField("foreign_born", FloatType(), False),
        StructField("average_household-size", FloatType(), False),
        StructField("state_code", StringType(), False),
        StructField("race", StringType(), False),
        StructField("count", IntegerType(), False)

    ])
    df_spark = spark.createDataFrame(df, schema=schema)
    df_spark.write.mode("overwrite").parquet(output)
    df_spark = spark.read.parquet(output)
    print(f"Staging Completed ")
    return df_spark


def staging_country_data(spark, path1, target):
    """
    Description: This function can be used to read and copy the country data file and
    loads it into the staging directory

    Arguments:
        spark: spark session
        path1: file path to countries.csv file downloaded from source https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes
        target: destination directory

    Returns:
        data: country spark df
    """
    print("Staging Started")

    df1 = pd.read_csv(path1)
    # countries = map_country(path2)

    df2 = pd.DataFrame(list(countries.items()), columns=['code', 'name'])
    df2 = df2[df2.name != 'Kosovo']
    df2 = df2[df2.name != 'Zaire']

    name_1, alpha_2, alpha_3, official_name, official_code = list(), list(), list(), list(), list()

    for i, row in df2.iterrows():
        # print(row['name'])
        b, c, d, e, = get_name(row['name'])
        alpha_2.append(b)
        alpha_3.append(c)
        official_name.append(d)
        official_code.append(e)

    df2['alpha_2'] = alpha_2
    df2['alpha_3'] = alpha_3
    df2['official_name'] = official_name
    df2['official_code'] = official_code

    df2['region'] = df2['alpha_3'].apply(lambda x: df1[df1['alpha-3'] == x].region.values)
    df2['region'] = df2['region'].astype(str).str.replace('\[|\]|\'', '')

    df2.to_csv(target + 'csv/country_processed.csv', index=False)

    output = target + 'parquet/'

    print("Spark Processing")
    schema = StructType([
        StructField("code", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("alpha_2", StringType(), True),
        StructField("alpha_3", StringType(), True),
        StructField("official_name", StringType(), True),
        StructField("official_code", StringType(), True),
        StructField("region", StringType(), True)

    ])
    df_spark = spark.createDataFrame(df2, schema=schema)
    df_spark.write.mode("overwrite").parquet(output)
    df_spark = spark.read.parquet(output)
    df_spark.show(5)
    print(f"Staging Completed ")
    return df_spark


def process_airport_table_data(spark, path, output_data):
    """
        Description: This function loads airport data from airport staging files, processes and creates a
        dimension (airport) table and then again stores as parquet file

        Parameters:
            spark       : Spark Session
            path  : staging airport parquet file path
            output_data : output path were dimensional tables in parquet format will be stored

        Returns:
            data: airport spark df
    """


    airport_table = spark.read.parquet(path)
    airport_table.write.mode("overwrite").parquet(output_data)
    airport_table = spark.read.parquet(output_data)
    return airport_table


def process_entry_table_data(path, output):
    """
       Description: This function extracts and transforms entry data from i94 immigration staging files, processes and
       creates a Fact (entry) Table then stores as parquet file

       Parameters:
           path: staged i94 immigration path
           output : output path were fact table in parquet format will be stored

       Returns:
           data: entry spark df
   """
    df = spark.read.parquet(path).withColumn("time_id", get_timestamp("arrdate"))
    df.createOrReplaceTempView("entry")
    entry_df = spark.sql(
        """
        select distinct int(cicid) as entry_id,
                        airline as airline_id,
                        string(fltno) as flight_id,
                        i94port as airport_id, 
                        int(admnum) as passenger_id, 
                        get_country(i94cit) as country_id,
                        time_id, 
                        hour(time_id) as hour, 
                        day(time_id) as day,
                        weekofyear(time_id) as week,
                        month(time_id) as month, 
                        year(time_id) as year,
                        dayofweek(time_id) as weekday
        from entry
        ORDER BY time_id
        """
    )
    entry_df.printSchema()
    entry_df.write.mode("overwrite").partitionBy("year", "month").parquet(output)
    entry_df = spark.read.parquet(output)
    return entry_df


def process_passenger_table_data(path, output):
    """
       Description: This function extracts and transforms passenger data from immigration staging files, processes and
       creates the dimension (passenger) table and stores as parquet file

       Parameters:
           path: staged i94 immigration path
           output: output path were dimensional tables in parquet format will be stored

       Returns:
           data: airport spark df
   """


    df = spark.read.parquet(path).withColumn("time_id", get_timestamp("arrdate"))
    df.createOrReplaceTempView("passenger")

    query = """

    SELECT DISTINCT admnum as passenger_id, airline, string(fltno) as flight_no, i94port as airport, 
                    get_country(i94cit) as departure_country, gender, i94visa as purpose_of_travel, i94bir as age
    FROM passenger
    """
    passenger = spark.sql(query)
    passenger.printSchema()
    passenger.write.mode("overwrite").parquet(output)
    passenger = spark.read.parquet(output)
    return passenger


def process_time_table_data(path, output):
    """
       Description: This function extracts and transforms time data from immigration staging files, processes and
       creates the dimension (time) table and stores as parquet file

       Parameters:
           path: staged i94 immigration spark path
           output: output path were dimensional tables in parquet format will be stored

       Returns:
           data: time spark df
   """
    df = spark.read.parquet(path).withColumn("time_id", get_timestamp("arrdate"))
    df.createOrReplaceTempView("time")
    query = """

    SELECT DISTINCT  time_id, hour(time_id) as hour, day(time_id) as day,
                     weekofyear(time_id) as week, month(time_id) as month, year(time_id) as year,
                     dayofweek(time_id) weekday

    FROM time
    ORDER BY time_id
    """
    time_df = spark.sql(query)
    time_df.printSchema()
    time_df.write.mode("overwrite").partitionBy("year", "month").parquet(output)
    time_df = spark.read.parquet(output)
    return time_df


def process_country_table_data(spark, input_data, output):
    """
       Description: This function extracts and transforms country data from immigration staging files,
       processes and creates the dimension (country) table and stores as parquet file

       Parameters:
            spark       : Spark Session
            input_data  : staging airport parquet file
            output : output path were dimensional country table in parquet format will be stored

       Returns:
            data: country spark df
   """
    df = spark.read.parquet(input_data)
    df.createOrReplaceTempView("country")
    query = """

    SELECT DISTINCT code as country_id, 
                    name as country_name,  
                    alpha_2 , alpha_3, official_name,
                    official_code as iso_code, 
                    region
    FROM country
    ORDER BY country_name
    """
    country_df = spark.sql(query)
    country_df.printSchema()
    output = output
    country_df.write.mode("overwrite").parquet(output)
    country_df = spark.read.parquet(output)
    return country_df




def main():
    """
    Description:
                1) Extract and Stage Airport, Country, Cities Demography and Immigration data from from source
                2) Transform and create dimensional and fact tables from staged files
                3) Perform Data Quality checks on each table
                4) Save each table in Parquet format
    """
    spark = create_spark()

    # Register spark udf

    spark.udf.register("get_country", get_country)
    spark.udf.register("get_timestamp", get_timestamp)
    spark.udf.register("to_int", to_int)
    spark.udf.register("to_str", to_str)


    # Stages
    # ******************

    # Staging Immigration data
    print("Staging Started")
    print("Staging Immigration Data")

    # we use only one month fro testing purposes
    directory = get_files(root)[0]
    try:
        staging_immigration_data(spark, directory, staging_immigration_path)
    except Exception as e:
        print("Issue in Staging Immigration Data")
        print(e)



    # Staging airport data
    print("Staging Airport Data")
    try:
        staging_airport_data(spark, source_airport, staging_airport_path)
    except Exception as e:
        print("Issue in Staging Airport Data")
        print(e)

    # Staging cities-demography data
    print("Staging city demography Data")
    try:
        staging_cities_demo_data(spark, source_cities_dem,
                                                       staging_cities_dem_path)
    except Exception as e:
        print("Issue in cities demography Data")
        print(e)

    # Staging Country data
    print("Staging country Data")
    try:
        staging_country_data(spark, source_country, staging_country_path)
    except Exception as e:
        print("Issue in staging country  Data")
        print(e)

    print("Staging Completed!!")
    print("*******************")
    # **************************

    
    print("Creating Dimension and Fact Tables Started")
    # Creating Tables

    # airport  table
    print("Processing and Creating airport Table")
    try:
        airport_table = process_airport_data(spark, staging_airport_path, airport_output_path+'parquet/')
    except Exception as e:
        print("Issue in creating airport table")
        print(e)


    # country table
    print("Processing and Creating Country Table")
    try:
        country_table = process_country_table_data(spark, staging_country_path, country_output_path)
    except Exception as e:
        print("Issue in creating country table")
        print(e)

    print("Processing and Creating Passenger Table")
    try:
        passenger_table = process_passenger_table_data(staging_immigration_path, passenger_output_path)
    except Exception as e:
        print("Issue in creating passenger table")
        print(e)


    print("Processing and Creating Time Table")
    try:
        time_table = process_time_table_data(staging_immigration_path, time_output_path)
    except Exception as e:
        print("Issue in creating passenger table")
        print(e)

    print("Processing and Creating Entry Table")
    try:
        entry_table = process_entry_table_data(staging_immigration_path, entry_output_path)
    except Exception as e:
        print("Issue in creating passenger table")
        print(e)
        
    print("All Tables Succesfully Created and Loaded into Storage Path!!")
    print("*******************")
    # **************************
    
    
    # Data Quality Checks
    print("Data Quality Check Started...")
    # Airport check
    print("Checking Airport Table")
    try:
        airport_table_check = data_quality_check(spark, airport_table,
                                                 metrics(["airport_id", "airport_name"], ["string", "string"]), table="Airport")
        print("Airport table check Passed")
    except Exception as e:
        print("Airport table check failed")
        print(e)
        
    # Country check
    print("Checking Country Table")
    try:
        country_table_check = data_quality_check(spark, country_table,
                                                 gen(["country_id", "country_name"], ["int", "string"]), table="Country")
        print("Country table check Passed")
    except Exception as e:
        print("Country table check failed")
        print(e)

    # Passenger check
    print("Checking Passenger Table")
    try:
        passenger_table_check = data_quality_check(spark, passenger_table,
                                                   metrics(["passenger_id", "flight_no"], ["int", "string"]), table="Passenger")
        print("Passenger table check Passed")
    except Exception as e:
        print("Passenger table check failed")
        print(e)

    # Time check
    print("Checking Time Table")
    try:
        time_table_check = data_quality_check(spark, time_table, metrics(["time_id", "hour"], ["timestamp", "int"]), table="Time")
        print("Time table check Passed")
    except Exception as e:
        print("Time table check failed")
        print(e)

    # Entry check
    print("Checking Entry Table")
    try:
        entry_table_check = data_quality_check(spark, entry_table, metrics(["time_id", "entry_id"], ["timestamp", "int"]), table="Entry")
        print("Entry table check Passed")
    except Exception as e:
        print("Entry table check failed")
        print(e)

    try:
        all_checks = {airport_table_check['code'], country_table_check['code'], passenger_table_check['code'],
                     time_table_check['code'], entry_table_check['code']}

    if 1 in all_checks:
        print("Data Quality Failed")
    else:
        print("Data Quality check on all tables was successful")


if __name__ == "__main__":
    main()
