import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from datetime import datetime, timedelta
import random

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="metadatadb",
    user="sdeuser",
    password="sdepassword",
    host="localhost",
    port="5432",
)
cur = conn.cursor()
# Set the search path to the rainforest schema
cur.execute("SET search_path TO rainforest")

# Commit the transaction to apply changes
conn.commit()

# Initialize Faker instance
fake = Faker()


# Function to generate fake user data
def generate_user_data(num_users):
    users = []
    for _ in range(num_users):
        username = fake.user_name()
        email = fake.email()
        is_active = fake.boolean(chance_of_getting_true=80)
        created_ts = fake.date_time_between(start_date="-2y", end_date="now")
        last_updated_by = None
        last_updated_ts = created_ts
        users.append(
            (username, email, is_active, created_ts, last_updated_by, last_updated_ts)
        )
    return users


# Function to generate fake seller data
def generate_seller_data(user_ids):
    sellers = []
    for user_id in user_ids:
        first_time_sold_timestamp = fake.date_time_between(
            start_date="-1y", end_date="now"
        )
        created_ts = first_time_sold_timestamp
        last_updated_by = None
        last_updated_ts = created_ts
        sellers.append(
            (
                user_id,
                first_time_sold_timestamp,
                created_ts,
                last_updated_by,
                last_updated_ts,
            )
        )
    return sellers


# Function to generate fake buyer data
def generate_buyer_data(user_ids):
    buyers = []
    for user_id in user_ids:
        first_time_purchased_timestamp = fake.date_time_between(
            start_date="-1y", end_date="now"
        )
        created_ts = first_time_purchased_timestamp
        last_updated_by = None
        last_updated_ts = created_ts
        buyers.append(
            (
                user_id,
                first_time_purchased_timestamp,
                created_ts,
                last_updated_by,
                last_updated_ts,
            )
        )
    return buyers


# Function to generate fake product data
def generate_product_data(num_products):
    products = []
    for _ in range(num_products):
        name = fake.sentence(nb_words=4)[:-1]
        description = fake.paragraph(nb_sentences=3)
        price = round(random.uniform(10.0, 500.0), 2)
        created_ts = fake.date_time_between(start_date="-2y", end_date="now")
        last_updated_by = None
        last_updated_ts = created_ts
        products.append(
            (name, description, price, created_ts, last_updated_by, last_updated_ts)
        )
    return products


# Function to generate fake seller_product data
def generate_seller_product_data(seller_ids, product_ids):
    seller_products = []
    for seller_id in seller_ids:
        products = random.sample(product_ids, random.randint(1, 10))
        for product_id in products:
            seller_products.append((seller_id, product_id))
    return seller_products


# Function to generate fake category data
def generate_category_data(num_categories):
    categories = []
    for _ in range(num_categories):
        name = fake.catch_phrase()
        created_ts = fake.date_time_between(start_date="-2y", end_date="now")
        last_updated_by = None
        last_updated_ts = created_ts
        categories.append((name, created_ts, last_updated_by, last_updated_ts))
    return categories


# Function to generate fake product_category data
def generate_product_category_data(product_ids, category_ids):
    product_categories = []
    for product_id in product_ids:
        categories = random.sample(category_ids, random.randint(1, 3))
        for category_id in categories:
            product_categories.append((product_id, category_id))
    return product_categories


# Function to generate fake order data
def generate_order_data(buyer_ids, num_orders):
    orders = []
    for _ in range(num_orders):
        buyer_id = random.choice(buyer_ids)
        order_date = fake.date_time_between(start_date="-1y", end_date="now")
        total_price = round(random.uniform(10.0, 1000.0), 2)
        created_ts = order_date
        last_updated_by = None
        last_updated_ts = created_ts
        orders.append(
            (
                buyer_id,
                order_date,
                total_price,
                created_ts,
                last_updated_by,
                last_updated_ts,
            )
        )
    return orders


# Function to generate fake order_item data
def generate_order_item_data(order_ids, seller_ids, product_ids):
    order_items = []
    for order_id in order_ids:
        seller_id = random.choice(seller_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        base_price = round(random.uniform(10.0, 500.0), 2)
        tax = round(base_price * 0.08, 2)  # Assuming an 8% tax
        created_ts = fake.date_time_between(start_date="-1y", end_date="now")
        last_updated_by = None
        last_updated_ts = created_ts
        order_items.append(
            (
                order_id,
                product_id,
                seller_id,
                quantity,
                base_price,
                tax,
                created_ts,
                last_updated_by,
                last_updated_ts,
            )
        )
    return order_items


# Function to generate fake clickstream data
def generate_clickstream_data(user_ids, product_ids, order_ids):
    clickstreams = []
    for user_id in user_ids:
        event_types = ["view", "add_to_cart", "purchase"]
        for _ in range(random.randint(5, 20)):
            event_type = random.choice(event_types)
            product_id = (
                random.choice(product_ids) if event_type != "purchase" else None
            )
            order_id = random.choice(order_ids) if event_type == "purchase" else None
            timestamp = fake.date_time_between(start_date="-1y", end_date="now")
            created_ts = timestamp
            last_updated_by = None
            last_updated_ts = created_ts
            clickstreams.append(
                (
                    user_id,
                    event_type,
                    product_id,
                    order_id,
                    timestamp,
                    created_ts,
                    last_updated_by,
                    last_updated_ts,
                )
            )
    return clickstreams


# Generate and insert data into tables
num_users = 1000
num_products = 500

# Generate and insert user data
user_data = generate_user_data(num_users)
insert_query = 'INSERT INTO "User" (username, email, is_active, created_ts, last_updated_by, last_updated_ts) VALUES %s'
execute_values(cur, insert_query, user_data)
conn.commit()

# Get user IDs for other tables
cur.execute('SELECT user_id FROM "User"')
user_ids = [row[0] for row in cur.fetchall()]

# Generate and insert seller data
seller_data = generate_seller_data(user_ids)
insert_query = "INSERT INTO Seller (user_id, first_time_sold_timestamp, created_ts, last_updated_by, last_updated_ts) VALUES %s"
execute_values(cur, insert_query, seller_data)
conn.commit()

# Generate and insert buyer data
buyer_data = generate_buyer_data(user_ids)
insert_query = "INSERT INTO Buyer (user_id, first_time_purchased_timestamp, created_ts, last_updated_by, last_updated_ts) VALUES %s"
execute_values(cur, insert_query, buyer_data)
conn.commit()

# Generate and insert product data
product_data = generate_product_data(num_products)
insert_query = "INSERT INTO Product (name, description, price, created_ts, last_updated_by, last_updated_ts) VALUES %s"
execute_values(cur, insert_query, product_data)
conn.commit()


# Get seller IDs and product IDs for other tables
cur.execute("SELECT seller_id FROM Seller")
seller_ids = [row[0] for row in cur.fetchall()]
cur.execute("SELECT product_id FROM Product")
product_ids = [row[0] for row in cur.fetchall()]

# Generate and insert seller_product data
seller_product_data = generate_seller_product_data(seller_ids, product_ids)
insert_query = "INSERT INTO SellerProduct (seller_id, product_id) VALUES %s"
execute_values(cur, insert_query, seller_product_data)
conn.commit()

# Generate and insert category data
num_categories = 20
category_data = generate_category_data(num_categories)
insert_query = "INSERT INTO Category (name, created_ts, last_updated_by, last_updated_ts) VALUES %s"
execute_values(cur, insert_query, category_data)
conn.commit()

# Get category IDs for product_category table
cur.execute("SELECT category_id FROM Category")
category_ids = [row[0] for row in cur.fetchall()]

# Generate and insert product_category data
product_category_data = generate_product_category_data(product_ids, category_ids)
insert_query = "INSERT INTO ProductCategory (product_id, category_id) VALUES %s"
execute_values(cur, insert_query, product_category_data)
conn.commit()

# Get buyer IDs for order table
cur.execute("SELECT buyer_id FROM Buyer")
buyer_ids = [row[0] for row in cur.fetchall()]

# Generate and insert order data
num_orders = 5000
order_data = generate_order_data(buyer_ids, num_orders)
insert_query = 'INSERT INTO "Order" (buyer_id, order_date, total_price, created_ts, last_updated_by, last_updated_ts) VALUES %s'
execute_values(cur, insert_query, order_data)
conn.commit()

# Get order IDs for order_item table
cur.execute('SELECT order_id FROM "Order"')
order_ids = [row[0] for row in cur.fetchall()]

# Generate and insert order_item data
order_item_data = generate_order_item_data(order_ids, seller_ids, product_ids)
insert_query = "INSERT INTO OrderItem (order_id, product_id, seller_id, quantity, base_price, tax, created_ts, last_updated_by, last_updated_ts) VALUES %s"
execute_values(cur, insert_query, order_item_data)
conn.commit()

# Get user IDs for clickstream table
cur.execute('SELECT user_id FROM "User"')
user_ids = [row[0] for row in cur.fetchall()]

# Generate and insert clickstream data
clickstream_data = generate_clickstream_data(user_ids, product_ids, order_ids)
insert_query = "INSERT INTO Clickstream (user_id, event_type, product_id, order_id, timestamp, created_ts, last_updated_by, last_updated_ts) VALUES %s"
execute_values(cur, insert_query, clickstream_data)
conn.commit()

# Close database connection
cur.close()
conn.close()

# Close database connection
cur.close()
conn.close()
