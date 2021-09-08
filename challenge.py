import pandas as pd
from datetime import date
import psycopg2
import os.path
from sqlalchemy import create_engine


today = date.today()

path_database = f'./data/postgres/{today}/'
path_csv = f"./data/csv/{today}/"


def db_connection():
    
    connection = "host={} dbname={} user={} password={}".format('127.0.0.1', 'northwind', 'northwind_user', 'thewindisblowing')

    try:  
        conn = psycopg2.connect(connection)
        print('Connection successfull')

        if os.path.isdir(path_database) and os.path.isdir(path_csv):
            print ('Already exists this path!')
        else:
            os.makedirs(path_csv)
            os.makedirs(path_database)
                    
        table_csv = pd.read_csv("./data/order_details.csv")
        table_csv.to_csv(path_csv+'order_details', index = False)

        # Open a cursor to perform database operations
        cur = conn.cursor()
    
        select = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema, table_name;"
        cur.execute(select)

        query_tables = cur.fetchall()

        for table_name in query_tables: 
            nameTable = table_name[0]
            df_database = pd.read_sql("SELECT * FROM {}".format(nameTable), conn)
            df_database.to_csv(path_database+nameTable, index = False)
        
        print('Save successfull')
    
    except psycopg2.DatabaseError:
        print('Connection Error!')


if __name__ == "__main__":
    
    db_connection()