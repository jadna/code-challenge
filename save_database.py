import os
import csv
import sys
from datetime import date
import mysql.connector
from mysql.connector import errorcode

#Paths
today = date.today()
path_postgres = f'./data/postgres/{today}'
path_csv = f"./data/csv/{today}"

def connect_database_mysql():

    try:
        connection = mysql.connector.connect(user='user', password='password', database='db_challenge')

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('Something is wrong.')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print('Database does not exist.')
        else:
            print('Error connecting to Database.')
        sys.exit()

    print('Successful')
    return connection

def execution():

    if os.path.exists(path_csv) and os.path.exists(path_postgres):

        connection = connect_database_mysql()
        cur = connection.cursor()

        try:
            for path in [path_postgres, path_csv]:

                for file_name in os.listdir(path):
                    print(path)
                    print(file_name)

                    table_name = file_name.split('.')[0]
                    file_path = f'{path}/{file_name}'

                    cur.execute(f'DELETE FROM {table_name};')
            
                    with open(file_path, newline='') as file_csv:
                        reader = csv.DictReader(file_csv)
                        for read_row in reader:
                            row = {}
                            for key in read_row.keys():
                                if read_row[key] != '':
                                    row[key] = read_row[key]

                            collums = ','.join(row.keys())
                            values = '", "'.join(row.values())

                            sql = f'INSERT INTO {table_name} ({collums}) VALUES ("{values}");'
                            cur.execute(sql)

        except Exception as e:
            print(f'An exception occurre: {e}')
            sys.exit()

        connection.commit()
        cur.close()
        connection.close()

    else:
        print('Fail step one.')

def query_orders():

    connection = connect_database_mysql()
    print(connection)
    cur = connection.cursor()

    #while True:

    sql = '''SELECT orders.order_id, 
    orders.order_date, 
    orders.shipped_date, 
    orders.freight, orders.ship_name, 
    orders.ship_address, 
    orders.ship_city, 
    orders.ship_region, 
    orders.ship_postal_code, 
    orders.ship_country, 
    details.unit_price, 
    details.quantity, 
    details.discount 
    FROM orders orders 
    INNER JOIN order_details details ON details.order_id = orders.order_id'''
    
    try:
        cur.execute(sql)

        headers = [header[0] for header in cur.description]
        res = cur.fetchall()
        print(res)

        with open(f'./data/query_final.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(res)

    except Exception as e:
        print(e)