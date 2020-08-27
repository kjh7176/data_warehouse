import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from iac import get_endpoint


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    host = get_endpoint()
    conn_string = "host={} dbname={} user={} password={} port={}".format(host, *config['CLUSTER'].values())
    print(conn_string)
    
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