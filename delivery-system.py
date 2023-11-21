import json
import psycopg2
from psycopg2 import pool



# Create a connection pool
conn_pool = psycopg2.pool.SimpleConnectionPool(
    1,  # minconn
    20,  # maxconn
    database="your_database",
    user="your_username",
    password="your_password",
    host="localhost",
    port="5432"
)

def reserve_order_items(cursor, order_id, order_items, zipcode):
    for item in order_items:
        cursor.execute("SELECT * FROM inventory WHERE med_id = %s AND order_id IS NULL AND zip_code = %s FOR UPDATE SKIP LOCKED LIMIT %s", (item['medicine_id'], zipcode, item['quantity']))
        rows = cursor.fetchall()
        if len(rows) < item['quantity']:
            raise Exception("Not sufficient items")
        else:
            cursor.executemany("UPDATE inventory SET order_id = %s WHERE uuid = %s AND zip_code = %s", [(order_id, row[0], zipcode) for row in rows])

def assign_agent(cursor, order_id, zipcode):
    cursor.execute("SELECT * FROM agent WHERE order_id IS NULL AND zip_code = %s FOR UPDATE SKIP LOCKED LIMIT 1", (zipcode,))
    agent = cursor.fetchone()
    if agent is None:
        raise Exception("No agent available")
    else:
        cursor.execute("UPDATE agent SET order_id = %s WHERE agent_id = %s AND zip_code = %s", (order_id, agent[0], zipcode))


def process_order(json_data, order_id):
    # Parse the JSON data
    data = json.loads(json_data)

    # Get a connection from the pool
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    try:
        # Start the transaction
        cursor.execute("BEGIN")

        # Reserve the order for each item in the order
        reserve_order_items(cursor, order_id, data['orderItems'])

        # Find the first agent with null or no order_id and assign the order id to this table
        assign_agent(cursor, order_id)

        # Commit the transaction
        conn.commit()
    except Exception as e:
        # Rollback the transaction in case of any errors
        conn.rollback()
        print("Not sufficient items")
    finally:
        # Close the database connection
        conn_pool.putconn(conn)

def store_order_details(json_data):
    # Parse the JSON data
    data = json.loads(json_data)
    customer_id = data['customer_id']
    zipcode = data['zipcode']

    # Get a connection from the pool
    conn = conn_pool.getconn()
    cursor = conn.cursor()

    try:
        # Start the transaction
        cursor.execute("BEGIN")

        # Generate a sequential order_id
        cursor.execute("SELECT nextval('order_id_seq')")
        order_id = cursor.fetchone()[0]

        # Insert the order details into the ORDER table with status as 'pending' and agent_id as null
        cursor.execute("INSERT INTO ORDER (order_id, customer_id, zipcode, status, agent_id) VALUES (%s, %s, %s, %s, %s)", (order_id, customer_id, zipcode, 'pending', None))
        for item in data['items']:
            cursor.execute("INSERT INTO ORDER_ITEM (order_id, med_id, quantity, zip_code) VALUES (%s, %s, %s, %s)", (order_id, item['med_id'], item['quantity'], zipcode))
        # Commit the transaction
        conn.commit()

        # Return the order_id
        return order_id
    except Exception as e:
        # Rollback the transaction in case of any errors
        conn.rollback()
        print("Failed to store order details")
    finally:
        # Return the connection back to the pool
        conn_pool.putconn(conn)


def main():
    # Open a file and read the order details
    with open('order.json') as json_file:
        json_data = json_file.read()

    # Store the order details in the database
    order_id = store_order_details(json_data)

    # Process the order
    process_order(json_data, order_id)
