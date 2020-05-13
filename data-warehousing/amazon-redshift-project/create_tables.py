import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Removes previously created tables
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables using create queries written in sql_queries script
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    creates database connection using configurations defined in dwh.cg file, 
    runs the drop_tables and create_tables function then closes the database connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Connected to cluster')
    print('Droping tables')
    drop_tables(cur, conn)
    print('Creating tables')
    create_tables(cur, conn)
    print("Closing Connection")
    conn.close()


if __name__ == "__main__":
    main()