import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 to staging tables on Redshift using queries written in sql_queries script.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
     Inserts selected data from staging tables to analytics (dimensional) tables on Redshift.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    try:
        print("Loading data from S3 into staging tables....")
        load_staging_tables(cur, conn)
        print("Data succesfully loaded into staging tables")
    except Exception as e:
        print(e)
    
    try:
        print("Inserting selected data from staging tables to analytics (dimensional) tables...")
        insert_tables(cur, conn)
        print("Selected Data succesfully loaded into dimensional tables")
    except Exception as e:
        print(e)
    
    print("Closing Connection")
    conn.close()


if __name__ == "__main__":
    main()