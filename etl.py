import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from iac import get_endpoint


def load_staging_tables(cur, conn):
    """
    Loads data from S3 to staging tables on Redshift cluster.
    
    Params:
    - cur: cursor for db connection to execute queries
    - conn: object for db connection 
    """
    
    # execute queries in copy_table_queries
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables to fact and dimension tables.
    
    Params:
    - cur: cursor for db connection to execute queries
    - conn: object for db connection 
    """
    
    # execute queries in insert_table_queries
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to the Redshift cluster.
    Loads data to staging tables.
    Insert data into analytics tables.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    host = get_endpoint()
    conn_string = "host={} dbname={} user={} password={} port={}".format(host, *config['CLUSTER'].values())
    
    try:
        conn = psycopg2.connect(conn_string)
    except Exception as e:
        print(e)
    else:
        cur = conn.cursor()

        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

        conn.close()


if __name__ == "__main__":
    main()