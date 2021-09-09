import os
import csv
import shutil
import sys
import psycopg2
from datetime import date

#Paths
today = date.today()
path_postgres = f'./data/postgres/{today}'
path_csv = f"./data/csv/{today}"

def export_database(cur, path):

    sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    cur.execute(sql)
    query_tables = cur.fetchall()

    table_names = [table[0] for table in query_tables]

    for table_name in table_names:

        cur.execute(f'SELECT * FROM {table_name}')
        table_data = cur.fetchall()
        
        header_names= [header[0] for header in cur.description]
        print(header_names)

        with open(f'{path}/{table_name}.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header_names)
            writer.writerows(table_data)

        print(f'Exported table.')

def save_csv(path):

    shutil.copyfile('./data/order_details.csv', f'{path}/order_details.csv')
    print('Exported CSV File.')

def connect_database():

    try:
        connection = psycopg2.connect(host='localhost', database='northwind', user='northwind_user', password='thewindisblowing')

    except psycopg2.Error as e:
        print(f'Error connecting to Database!')
        sys.exit()

    print('Success connecting')
    return connection

def create_path(path):

    try:
        os.makedirs(path)

    except OSError as error:
        print('Error creating folders.')
        sys.exit()

def execution():

    if not os.path.exists(path_postgres):
        print('Start script today.')

        #Connection to DB
        connection = connect_database()
        cur = connection.cursor()

        try:
            create_path(path_postgres)
            export_database(cur, path_postgres)

        except Exception as e:
            
            print(f'Error occurred exporting database ({path_postgres}).')
            os.rmdir(path_postgres)
            sys.exit()      

        try:
            create_path(path_csv)
            save_csv(path_csv)

        except Exception as e:
            print(f'Error occurred exporting csv ({path_csv}).')
            os.rmdir(path_csv)
            sys.exit() 
    else:
        print('Script has already executed.')

    print('Executed.')