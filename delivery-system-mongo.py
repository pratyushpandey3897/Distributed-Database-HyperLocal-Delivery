from pymongo import MongoClient
import json

def create_collections():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['mydatabase']

    medicine_details = db['medicine_details']

    print("Collections created successfully!")
    return medicine_details

def insert_data(collection, data):
    collection.insert_many(data)

def read_data(collection, query):
    return collection.find_one(query)

def update_data(collection, query, new_data):
    collection.update_one(query, {"$set": new_data})

def delete_data(collection, query):
    collection.delete_one(query)

def get_medicine_price(collection, medicine_id, medicine_name):
    query = {"medicine_id": medicine_id, "name": medicine_name}
    medicine = read_data(collection, query)
    return medicine["price"] if medicine else None

def add_order(collection, customer_id, order_id):
    customer = collection.find_one({"customer_id": customer_id})

    if not customer:
        print(f"No customer found with id {customer_id}")
        return

    recent_orders = customer.get("recent_orders", [])
    older_orders = customer.get("older_orders", [])

    # If recent_orders already has 4 items, move the oldest one to older_orders
    if len(recent_orders) == 4:
        older_orders.append(recent_orders.pop(0))

    # Add the new order to recent_orders
    recent_orders.append(order_id)

    # Update the customer document
    collection.update_one({"customer_id": customer_id}, {"$set": {"recent_orders": recent_orders, "older_orders": older_orders}})

def get_customer_details(collection, customer_id):
    query = {"customer_id": customer_id}
    customer = read_data(collection, query)
    return customer

def get_warehouse_details(collection, warehouse_id):
    query = {"warehouse_id": warehouse_id}
    warehouse = read_data(collection, query)
    return warehouse

if __name__ == "__main__":
    medicine_details = create_collections()

    with open('medicine_data.json') as f:
        medicine_data = json.load(f)
    insert_data(medicine_details, medicine_data)

    # Fetch the price of a medicine
    medicine_id = "MED1234"
    medicine_name = "Paracetamol"
    price = get_medicine_price(medicine_details, medicine_id, medicine_name)
    print(f"The price of {medicine_name} is {price}")
