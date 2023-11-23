DATABASE = "delivery_system"
DELIVERY_AGENT_TABLE = "Delivery_Agent"
INVENTORY = "Inventory"
AGENT_ID = "agent_id"
AGENT_NAME = "agent_name"
ORDER_ID = "order_id"
ZIPCODE = "zip_code"
UUID = "uuid"
STATUS = "status"
WAREHOUSE_ID = "warehouse_id"
MED_ID = "med_id"
ORDER = "Orders"
ORDER_ITEM = "Order_Item"
CUSTOMER_ID = "customer_id"
MED_ID = "med_id"
QUANTITY = "quantity"

import psycopg2
from psycopg2 import extensions
import random
import logging

# logging.basicConfig(level=logging.INFO)
print("hello tables...")

def make_order(conn, zip_codes):
    print("Creating Order table...")
    try:
        print("Creating Order table...")
        # logging.info("Creating Order table...")
        zip_codes = min(100, zip_codes)
        cur = conn.cursor()
        table_creation_query = f"""
        CREATE TABLE {ORDER}(
            {CUSTOMER_ID} INT NOT NULL,
            {STATUS} VARCHAR(10) NOT NULL,
            {ORDER_ID} VARCHAR(10),
            {ZIPCODE} INT NOT NULL,
            {AGENT_ID} INT
        ) PARTITION BY LIST ({ZIPCODE});"""
   
     
        create_sequence_query = f"CREATE SEQUENCE order_id_seq START 9000;"
        cur.execute(create_sequence_query)
        print(table_creation_query)
        cur.execute(table_creation_query)
        zip_c =  85200
        for i in range (0, zip_codes):
            partition_creation_query = f"CREATE TABLE Order_zip_code_{zip_c+i} PARTITION OF {ORDER} FOR VALUES IN ({zip_c+i});"
            cur.execute(partition_creation_query)
            # create index if not exists
            index_creation_query = f"CREATE INDEX IF NOT EXISTS Order_zip_code_{zip_c+i}_index ON Order_zip_code_{zip_c+i}  ({ORDER_ID});"
            cur.execute(index_creation_query)
        conn.commit()
    except Exception as e:
        logging.error(f"Error creating Order table: {e}")


def make_order_item(conn, zip_codes):
    print("Creating Order_Item table...")
    try:
        print("Creating Order_Item table...")
        # logging.info("Creating Order_Item table...")
        zip_codes = min(100, zip_codes)
        cur = conn.cursor()
        table_creation_query = f"""
        CREATE TABLE {ORDER_ITEM}(
            {ORDER_ID} VARCHAR(10),
            {MED_ID} INT NOT NULL,
            {QUANTITY} INT NOT NULL,
            {ZIPCODE} INT NOT NULL
        ) PARTITION BY LIST ({ZIPCODE});"""
        cur.execute(table_creation_query)

        #create index if not exists
        index_creation_query = f"CREATE INDEX IF NOT EXISTS {ORDER_ITEM}_zip_code_index ON {ORDER_ITEM} ({ZIPCODE});"
        cur.execute(index_creation_query)
        zip_c =  85200
        for i in range (0, zip_codes):
            partition_creation_query = f"CREATE TABLE Order_Item_zip_code_{zip_c+i} PARTITION OF {ORDER_ITEM} FOR VALUES IN ({zip_c+i});"
            cur.execute(partition_creation_query)
            # create index if not exists
            index_creation_query = f"CREATE INDEX IF NOT EXISTS Order_Item_zip_code_{zip_c+i}_index ON Order_Item_zip_code_{zip_c+i} ({ORDER_ID});"
            cur.execute(index_creation_query)

        conn.commit()
    except Exception as e:
        logging.error(f"Error creating Order_Item table: {e}")


def create_database(dbname, conn):
    print("Creating database...")
    cur = conn.cursor()
    conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    create = "create database " + dbname + ";"
    drop_database_query = f"DROP DATABASE IF EXISTS {dbname} WITH (FORCE);"
    cur.execute(drop_database_query)
    cur.execute(create)
    conn.commit()
    conn.close()


def make_delivery_agent(conn, zip_codes):
    print("Creating Delivery_Agent table...")
    zip_codes = min(100, zip_codes)

    cur = conn.cursor()
    table_creation_query = f"""
    CREATE TABLE {DELIVERY_AGENT_TABLE}(
        {AGENT_ID} SERIAL,
        {AGENT_NAME} VARCHAR(255) NOT NULL,
        {ORDER_ID} VARCHAR(10),
        {ZIPCODE} INT NOT NULL
    ) PARTITION BY LIST ({ZIPCODE});"""

    cur.execute(table_creation_query)

    index_creation_query = f"CREATE INDEX IF NOT EXISTS Delivery_Agent_zip_code_index ON Delivery_Agent ({ZIPCODE});"   
    cur.execute(index_creation_query)

    zip_c =  85200
    for i in range (0, zip_codes):
        partition_creation_query = f"CREATE TABLE Delivery_Agent_zip_code_{zip_c+i} PARTITION OF Delivery_Agent FOR VALUES IN ({zip_c+i});"
        cur.execute(partition_creation_query)
        # create index if not exists
        index_creation_query = f"CREATE INDEX IF NOT EXISTS Delivery_Agent_zip_code_{zip_c+i}_index ON Delivery_Agent_zip_code_{zip_c+i}({ORDER_ID});"
        cur.execute(index_creation_query)

    conn.commit()


def insert_data_agent(conn, zip_codes):
    print("Inserting data into Delivery_Agent table...")
    zip_codes = min(2, zip_codes)
    zip_c =  85200
        
    names = ["Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Henry", "Ivy", "Jack",
             "Kate", "Leo", "Mia", "Noah", "Olivia", "Peter", "Quinn", "Ruby", "Sam", "Tom"]

    cur = conn.cursor()
    for i in range (0, zip_codes):
        for _ in range(1200):  # Create 4 agents for each zip code
            name = random.choice(names)
            insert_data_query = f"INSERT INTO {DELIVERY_AGENT_TABLE} ({AGENT_NAME},{ORDER_ID},{ZIPCODE}) VALUES ('{name}',NULL,{zip_c+i}) RETURNING {AGENT_ID}"
            cur.execute(insert_data_query)

    conn.commit()


def make_inventory(conn, zipcodes):
    print("Creating Inventory table...")
    zip_codes = min(100, zipcodes)

    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")

    table_creation_query = f"""
    CREATE TABLE Inventory (
        {UUID} serial,
        {WAREHOUSE_ID} INT NOT NULL,
        {ORDER_ID} VARCHAR(10),
        {MED_ID} INT NOT NULL,
        {ZIPCODE} INT NOT NULL,
        PRIMARY KEY ( {ZIPCODE}, {MED_ID}, {UUID})
    ) PARTITION BY LIST ({ZIPCODE});
                               """

    cur.execute(table_creation_query)
    

    zip_c =  85200
    for i in range (0, zip_codes):
        partition_creation_query = f"CREATE TABLE Inventory_zip_code_{zip_c+i} PARTITION OF {INVENTORY} FOR VALUES IN ({zip_c+i});"
        cur.execute(partition_creation_query)
        # create index if not exists
        index_creation_query = f"CREATE INDEX IF NOT EXISTS Inventory_zip_code_{zip_c+i}_index ON Inventory_zip_code_{zip_c+i} ({MED_ID});"
        # cur.execute(index_creation_query)
        

    conn.commit()


def insert_data_inventory(conn, zip_codes, warehouse, medicine, rows_inventory):
    print("Inserting data into Inventory table...")
    zip_codes = min(6, zip_codes)
    # zip_list = [f"{i:02}" for i in range(zip_codes)]
    warehouse_list = list(range(1, warehouse + 1))
    medicine_list = list(range(1, medicine + 1))
    cur = conn.cursor()
    zip_c =  85200
    for i in range (0, zip_codes):
        for medi in medicine_list:
            for _ in range(1400):  # Ensure 10 counts for each med_id
                wh = random.choice(warehouse_list)
                insert_data_query = f"INSERT INTO {INVENTORY} ({WAREHOUSE_ID},{ORDER_ID},{MED_ID},{ZIPCODE}) VALUES ({wh},NULL,{medi},{zip_c+i})"
                cur.execute(insert_data_query)

    conn.commit()


if __name__ == '__main__':
    zipcodes = 10
    warehouse = 10
    medicine = 10
    rows_inventory = 1000;
    print("hello tables...")
    try:

        conn = psycopg2.connect(database="postgres", user="postgres", host='localhost', password="postgres", port=5432)
        create_database(DATABASE, conn)
        conn = psycopg2.connect(database=DATABASE, user="postgres", host='localhost', password="postgres", port=5432)
        print(conn)
        make_order(conn, zipcodes)
        make_order_item(conn, zipcodes)
        make_delivery_agent(conn, zipcodes)
        insert_data_agent(conn, zipcodes)
        make_inventory(conn, zipcodes)
        insert_data_inventory(conn, zipcodes, warehouse, medicine, rows_inventory)
        conn.close()
    except Exception as e:
        print(e)
        print("Error")
        logging.error(f"Error creating database: {e}")

