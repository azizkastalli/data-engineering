import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This drops all the tables from the redshift database.
    
    args:
        cur (object): psycopg2 cursor object.
        conn (object): psycopg2 connection object.
    
    returns:
        None.
    """
        
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates all necessary tables for the etl process 
    in the redshift database.
    
    args:
        cur (object): psycopg2 cursor object.
        conn (object): psycopg2 connection object.
    
    returns:
        None.
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()