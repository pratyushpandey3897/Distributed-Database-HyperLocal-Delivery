import json
import threading
import psycopg2
from psycopg2 import pool



# Create a connection pool
conn_pool = psycopg2.pool.SimpleConnectionPool(
    1,  # minconn
    100,  # maxconn
    database="delivery_system",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

import time

import time

import time

def reserve_order_items(cursor, order_id, order_items, zip_code):
    start_time = time.time()  # Start the timer
    try:
        uuids = []
        retry_count = 1
        for item in order_items:
            retries = 0
            
            while retries < retry_count:  # Set a limit for the number of retries
                cursor.execute(" SELECT * FROM inventory WHERE med_id = %s AND order_id IS NULL AND zip_code = %s FOR UPDATE LIMIT %s", (item['med_id'], zip_code, item['quantity']))
                rows = cursor.fetchall()
                if len(rows) < item['quantity']:
                    # Not enough items, release locks and try again
                    # cursor.execute("ROLLBACK")
                    # time.sleep(0.1)  # Wait for a short delay before trying again
                    retries += 1
                else:
                    cursor.executemany("UPDATE inventory SET order_id = %s WHERE uuid = %s AND zip_code = %s", [(order_id, row[0], zip_code) for row in rows])
                    uuids.extend([row[0] for row in rows])
                    break  # Exit the loop once the necessary number of rows have been locked
            if retries == retry_count:
                raise Exception("Failed to reserve items after {retry_count} retries")
        end_time = time.time()  # End the timer
        print(f"Time taken to reserve order items for order_id {order_id}: {end_time - start_time} seconds")
        return uuids
    except Exception as e:
        print(f"Error in reserve_order_items: {e}")
        return None

def assign_agent(cursor, order_id, zip_code):
    start_time = time.time()  # Start the timer
    try:
        cursor.execute("SELECT * FROM Delivery_Agent WHERE order_id IS NULL AND zip_code = %s FOR UPDATE LIMIT 1", (zip_code,))
        agent = cursor.fetchone()
        if agent is None:
            raise Exception("No agent available")
        else:
            cursor.execute("UPDATE Delivery_Agent SET order_id = %s WHERE agent_id = %s AND zip_code = %s", (order_id, agent[0], zip_code))
            end_time = time.time()  # End the timer
            print(f"Time taken to assign agent for order_id {order_id}: {end_time - start_time} seconds")
            return agent[0]
    except Exception as e:
        print(f"Error in assign_agent: {e}")
        return None

def process_order(json_data, order_id):
    # Parse the JSON data
    data = json.loads(json_data)
    # print(f"Processing order for customer id: {data['customer_id']}, order_id: {order_id}")

    # Get a connection from the pool
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    try:
        # Start the transaction
        cursor.execute("BEGIN")

        # Reserve the order for each item in the order
        uuids = reserve_order_items(cursor, order_id, data['items'], data["zip_code"])
        if uuids is None:
            raise Exception("Failed to reserve order items")

        # Find the first agent with null or no order_id and assign the order id to this table
        agent_id = assign_agent(cursor, order_id, data["zip_code"])
        if agent_id is None:
            raise Exception("Failed to assign agent")

        # Commit the transaction
        conn.commit()

        return True, uuids, agent_id
    except Exception as e:
        # Rollback the transaction in case of any errors
        conn.rollback()
        print(f"Error in process_order: {e}")
        return False, [], None
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
        # cursor.execute("set enable_index_onlyscan TO off")
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



def start_order_on_new_thread(json_data):
    # Start a new thread to process the order
    try:
        order_id = store_order_details(json_data)
        success, uuids, agent_id = process_order(json_data, order_id)    
        print(f"Success: {success}, UUIDs: {uuids}, Agent ID: {agent_id}")
        return success
    except Exception as e:
        print(f"Error in start_order_on_new_thread: {e}")
        return False




import time

from concurrent.futures import ThreadPoolExecutor

def main():
    start_time = time.time()
    successful_orders = 0
    unsuccessful_orders = 0
    total_qty =0

    with open('sample_order.json') as json_file:
        orders = json.loads(json_file.read())

    with ThreadPoolExecutor() as executor:
        futures = []
        for order in orders:
            json_data = json.dumps(order)
            order_id = store_order_details(json_data)
            total_qty += (order['items'][0].get('quantity'))
            future = executor.submit(process_order, json_data, order_id)
            futures.append(future)

        for future in futures:
            if future.result()[0]:
                # print(future.result()[0])
                successful_orders += 1
            else:
                unsuccessful_orders += 1

    end_time = time.time()
    print(f"Time taken to complete all threads: {end_time - start_time} seconds")
    print(f"Number of successful orders: {successful_orders}")
    print(f"Number of unsuccessful orders: {unsuccessful_orders}")
    print(f"Number of orders: {len(orders)}")
    print(f"Total quantity of items ordered: {total_qty}")


main()