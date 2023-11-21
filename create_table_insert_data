DATABASE = "delivery_system"
DELIVERY_AGENT_TABLE = "Delivery_Agent"
INVENTORY = "Inventory"
AGENT_ID = "agent_id"
AGENT_NAME="agent_name"
ORDER_ID = "order_id"
ZIPCODE = "zip_code"
UUID = "uuid"
WAREHOUSE_ID = "warehouse_id"
MED_ID = "med_id"


import psycopg2
from psycopg2 import extensions
import random

def create_database(dbname,conn):

    cur = conn.cursor()
    conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    create = "create database " + dbname + ";"
    drop_database_query = f"DROP DATABASE IF EXISTS {dbname};"
    cur.execute(drop_database_query)
    cur.execute(create)
    conn.commit()
    conn.close()

def make_delivery_agent(conn,zip_codes):

    zip_codes = min(100,zip_codes)

    cur = conn.cursor()
    table_creation_query = f"""
    CREATE TABLE {DELIVERY_AGENT_TABLE}(
        {AGENT_ID} SERIAL,
        {AGENT_NAME} VARCHAR(255) NOT NULL,
        {ORDER_ID} VARCHAR(10),
        {ZIPCODE} VARCHAR(5) NOT NULL
    ) PARTITION BY LIST ({ZIPCODE});"""

    cur.execute(table_creation_query)
    
    zip_list = [f"{i:02}" for i in range(zip_codes)]
    for z in zip_list:
        partition_creation_query = f"CREATE TABLE Delivery_Agent_zip_code_852{z} PARTITION OF Delivery_Agent FOR VALUES IN ('852{z}');"
        cur.execute(partition_creation_query)

    
    conn.commit()
    

def insert_data_agent(conn,zip_codes):

    zip_codes = min(100,zip_codes)
    zip_list = [f"{i:02}" for i in range(zip_codes)]
    names = ["Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Henry", "Ivy", "Jack",
         "Kate", "Leo", "Mia", "Noah", "Olivia", "Peter", "Quinn", "Ruby", "Sam", "Tom"]
    
    cur = conn.cursor()
    for name in names:
        zp = random.choice(zip_list)
        insert_data_query = f"INSERT INTO {DELIVERY_AGENT_TABLE} ({AGENT_NAME},{ORDER_ID},{ZIPCODE}) VALUES ('{name}',NULL,'852{zp}') RETURNING {AGENT_ID}"
        cur.execute(insert_data_query)
    
    conn.commit()

def make_inventory(conn,zipcodes):

    zip_codes = min(100,zipcodes)

    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")

    table_creation_query = f"""
    CREATE TABLE Inventory (
        {UUID} UUID DEFAULT uuid_generate_v4(),
        {WAREHOUSE_ID} INT NOT NULL,
        {ORDER_ID} VARCHAR(10),
        {MED_ID} INT NOT NULL,
        {ZIPCODE} VARCHAR(10) NOT NULL,
        PRIMARY KEY ({UUID}, {ZIPCODE})
    ) PARTITION BY LIST ({ZIPCODE});
                               """

    cur.execute(table_creation_query)
    zip_list = [f"{i:02}" for i in range(zip_codes)]
    for z in zip_list:
        partition_creation_query = f"CREATE TABLE Inventory_zip_code_852{z} PARTITION OF {INVENTORY} FOR VALUES IN ('852{z}');"
        cur.execute(partition_creation_query)

    
    conn.commit()

def insert_data_inventory(conn,zip_codes,warehouse,medicine,rows_inventory):

    zip_codes = min(100,zip_codes)
    zip_list = [f"{i:02}" for i in range(zip_codes)]
    warehouse_list=list(range(1,warehouse+1))
    medicine_list=list(range(1,medicine+1))
    cur = conn.cursor()
    for _ in range(rows_inventory):
        zp = random.choice(zip_list)
        wh = random.choice(warehouse_list)
        medi = random.choice(medicine_list)
        insert_data_query = f"INSERT INTO {INVENTORY} ({WAREHOUSE_ID},{ORDER_ID},{MED_ID},{ZIPCODE}) VALUES ({wh},NULL,{medi},'852{zp}')"
        cur.execute(insert_data_query)
    
    conn.commit()

if __name__ == '__main__':
    zipcodes = 10
    warehouse=10
    medicine=10
    rows_inventory=1000;


    conn = psycopg2.connect(database = "postgres", user = "postgres", host= 'localhost',password = "shlok1513",port = 5432)
    create_database(DATABASE,conn)
    conn = psycopg2.connect(database = DATABASE, user = "postgres", host= 'localhost',password = "shlok1513",port = 5432)
    make_delivery_agent(conn,zipcodes)
    insert_data_agent(conn,zipcodes)
    make_inventory(conn,zipcodes)
    insert_data_inventory(conn,zipcodes,warehouse,medicine,rows_inventory)
    conn.close()
