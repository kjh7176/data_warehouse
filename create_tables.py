import configparser
import psycopg2
from iac import create_cluster, delete_cluster
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(e)
        else:
            conn.commit()
            print("Droped table!")


def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(e)
        else:
            conn.commit()
            print("Created table!")
            


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    host = create_cluster()
    conn_string = "host={} dbname={} user={} password={} port={}".format(host, *config['CLUSTER'].values())
    print(conn_string)

    try:
        conn = psycopg2.connect(conn_string)
    except Exception as e:
         print(e)
    else:
        cur = conn.cursor()
        drop_tables(cur, conn)
        create_tables(cur, conn)
        conn.close()

if __name__ == "__main__":
    main()