import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads the sparkify songs and events staging tables 
    from an s3 bucket into redshift database.
    
    args:
        cur (object): psycopg2 cursor object.
        conn (object): psycopg2 connection object.
    
    returns:
        None.
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function inserts data from the events and songs staging tables 
    into the fact and dimension tables.
    
    args:
        cur (object): psycopg2 cursor object.
        conn (object): psycopg2 connection object.
    
    returns:
        None.
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()