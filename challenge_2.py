import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine
from datetime import date


# The initial data load (step 1) must be opened from a Postgres server
# The final data load (step 2) requires MySQL 

today = date.today()
directory = r"...\data\postgres\{}\{}.csv" # Choose a directory for the backup


def db_connection():  # (Step 1) Function to open the database and save the tables backup
    
    # Here you must know the credentials to connect the postegres database
    # This method is easier for running the code in different computers
    
    print('Insert host name/address')
    host = input()
    print('Insert host database name')
    dbname = input()
    print('Insert username')
    user = input()
    print('Insert password')
    password = input()

    dbconnection = "host={} dbname={} user={} password={}".format(host, dbname, user, password)
    print('Connecting into database...')
    
    try:  
        conn = psycopg2.connect(dbconnection)
        print('Connection successfull')
        
        print('Save data in local disk? Y or N')
        response = input()
        if response == "Y":
            
            csv_table = pd.read_csv("order_details.csv")
    
            cur = conn.cursor()
        
            s = ""
            s += "SELECT"
            s += " table_schema"
            s += ", table_name"
            s += " FROM information_schema.tables"
            s += " WHERE"
            s += " ("
            s += " table_schema = 'public'"
            s += " )"
            s += " ORDER BY table_schema, table_name;"
        
            cur.execute(s)
            list_tables = cur.fetchall()
        
        # Selecting each table name and saving them in local disk: 
            for t_name_table in list_tables: 
                table_name = t_name_table[1]
                df = pd.read_sql("select * from {}".format(table_name), conn)
                df.to_csv(directory.format(table_name, today))
        
            csv_table.to_csv(directory.format("order_details", today))
            
            print('Save successfull')
    
    except psycopg2.DatabaseError:
        print('Connection Error! Check login credentials')

        
directory = r"...\data\postgres\.csv" # Choose a directory to save the final query csv file

    
def final_db_load(): # (Step 2) Loading files in the final database and displaying orders and order_detail tables
    
    # Again you must know the credentials to connect in the MySQL server
    
    print("Insert username")
    user = input()
    print("Insert password")
    password = input()
    print('Insert host name/address')
    host = input()
    print('Insert database name')
    dbname = input()

    dbengine = "mysql+pymysql://{}:{}@{}/{}".format(user, password, host, dbname)
    engine = create_engine(dbengine)
    
    # Loading the backup files and loading into the final database:
    rootDir = r'...\data\postgres'  # Select the root file for the tables backup
    for dirName, subdirList, fileList in os.walk(rootDir, topdown=False):
        for fname in fileList:
            table_name = dirName.split('\\')[-1] 
            filepath = os.path.join(rootDir, dirName, fname)
            df = pd.read_csv(filepath)
            df.to_sql(table_name, con=engine, index = False)
        
    print("Show the orders and its details? Y or N")
    response = input()
    if response == "Y":
             
        orders = pd.read_sql("SELECT * FROM orders", engine)
        details = pd.read_sql("SELECT * FROM order_details", engine)
        
        print("Orders table:")
        
        orders.to_csv(directory)
        print("Orders details table:")

        details.to_csv(directory)

if __name__ == "__main__":

    db_connection()