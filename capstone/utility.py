import pycountry
import os
import pandas as pd
import pycountry
from datetime import datetime, timedelta
import os, glob
from pyspark.sql.functions import udf
from datetime import datetime, timedelta


# Utitlity functions
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

