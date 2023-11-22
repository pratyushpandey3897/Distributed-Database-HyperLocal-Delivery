import json
import psycopg2
from psycopg2 import pool



# Create a connection pool
conn_pool = psycopg2.pool.SimpleConnectionPool(
    1,  # minconn
    20,  # maxconn
    database="delivery_system",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

def reserve_order_items(cursor, order_id, order_items, zip_code):
    for item in order_items:
        cursor.execute("SELECT * FROM inventory WHERE med_id = %s AND order_id IS NULL AND zip_code = %s FOR UPDATE SKIP LOCKED LIMIT %s", (item['med_id'], zip_code, item['quantity']))
        rows = cursor.fetchall()
        if len(rows) < item['quantity']:
            raise Exception("Not sufficient items")
        else:
            cursor.executemany("UPDATE inventory SET order_id = %s WHERE uuid = %s AND zip_code = %s", [(order_id, row[0], zip_code) for row in rows])

def assign_agent(cursor, order_id, zip_code):
    cursor.execute("SELECT * FROM Delivery_Agent WHERE order_id IS NULL AND zip_code = %s FOR UPDATE SKIP LOCKED LIMIT 1", (zip_code,))
    agent = cursor.fetchone()
    if agent is None:
        raise Exception("No agent available")
    else:
        cursor.execute("UPDATE Delivery_Agent SET order_id = %s WHERE agent_id = %s AND zip_code = %s", (order_id, agent[0], zip_code))


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
        reserve_order_items(cursor, order_id, data['items'], data["zip_code"])

        # Find the first agent with null or no order_id and assign the order id to this table
        assign_agent(cursor, order_id, data["zip_code"])

        # Commit the transaction
        conn.commit()
    except Exception as e:
        # Rollback the transaction in case of any errors
        conn.rollback()
        print("Not sufficient items", e)
    finally:
        # Close the database connection
        conn_pool.putconn(conn)

def store_order_details(json_data):
    # Parse the JSON data
    data = json.loads(json_data)
    customer_id = data['customer_id']
    zip_code = data['zip_code']

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
        cursor.execute("INSERT INTO ORDERS (order_id, customer_id, zip_code, status, agent_id) VALUES (%s, %s, %s, %s, %s)", (order_id, customer_id, zip_code, 'pending', None))
        for item in data['items']:
            cursor.execute("INSERT INTO ORDER_ITEM (order_id, med_id, quantity, zip_code) VALUES (%s, %s, %s, %s)", (order_id, item['med_id'], item['quantity'], zip_code))
        # Commit the transaction
        conn.commit()

        # Return the order_id
        return order_id
    except Exception as e:
        # Rollback the transaction in case of any errors
        conn.rollback()
        print("Failed to store order details" , e)
    finally:
        # Return the connection back to the pool
        conn_pool.putconn(conn)


def main():
    # Open a file and read the order details
    with open('sample_order.json') as json_file:
        json_data = json_file.read()

    # Store the order details in the database
    order_id = store_order_details(json_data)

    # Process the order
    process_order(json_data, order_id)

main()
