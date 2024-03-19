CREATE SCHEMA IF NOT EXISTS rainforest;

SET search_path TO rainforest;

-- Create AppUser table
CREATE TABLE AppUser (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by INT ,
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Category table
CREATE TABLE Category (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by INT REFERENCES AppUser(user_id),
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Brand table
CREATE TABLE Brand (
    brand_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    country VARCHAR(255),
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by INT REFERENCES AppUser(user_id),
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Manufacturer table
CREATE TABLE Manufacturer (
    manufacturer_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(255),
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by INT REFERENCES AppUser(user_id),
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Product table
CREATE TABLE Product (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    brand_id INT REFERENCES Brand(brand_id),
    manufacturer_id INT REFERENCES Manufacturer(manufacturer_id),
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by INT REFERENCES AppUser(user_id),
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Ratings table
CREATE TABLE Ratings (
    ratings_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES Product(product_id),
    rating DECIMAL(3, 2),
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by INT REFERENCES AppUser(user_id),
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Seller table
CREATE TABLE Seller (
    seller_id SERIAL PRIMARY KEY,
    user_id INT UNIQUE REFERENCES AppUser(user_id) ON DELETE CASCADE,
    first_time_sold_timestamp TIMESTAMP,
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by INT REFERENCES AppUser(user_id),
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Buyer table
CREATE TABLE Buyer (
    buyer_id SERIAL PRIMARY KEY,
    user_id INT UNIQUE REFERENCES AppUser(user_id) ON DELETE CASCADE,
    first_time_purchased_timestamp TIMESTAMP,
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by INT REFERENCES AppUser(user_id),
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Order table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    buyer_id INT REFERENCES Buyer(buyer_id) ON DELETE CASCADE,
    order_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_price DECIMAL(10, 2) NOT NULL,
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create OrderItem table
CREATE TABLE OrderItem (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INT REFERENCES Product(product_id) ON DELETE CASCADE,
    seller_id INT REFERENCES Seller(seller_id) ON DELETE CASCADE,
    quantity INT NOT NULL,
    base_price DECIMAL(10, 2) NOT NULL,
    tax DECIMAL(10, 2) NOT NULL,
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create SellerProduct table
CREATE TABLE SellerProduct (
    seller_id INT REFERENCES Seller(seller_id) ON DELETE CASCADE,
    product_id INT REFERENCES Product(product_id) ON DELETE CASCADE,
    PRIMARY KEY (seller_id, product_id)
);

-- Create Clickstream table
CREATE TABLE Clickstream (
    event_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES AppUser(user_id) ON DELETE CASCADE,
    event_type VARCHAR(20) NOT NULL,
    product_id INT REFERENCES Product(product_id),
    order_id INT REFERENCES orders(order_id),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ProductCategory (
    product_id INTEGER,
    category_id INTEGER,
    PRIMARY KEY (product_id, category_id),
    FOREIGN KEY (product_id) REFERENCES Product(product_id),
    FOREIGN KEY (category_id) REFERENCES Category(category_id),
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by INT REFERENCES AppUser(user_id),
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
