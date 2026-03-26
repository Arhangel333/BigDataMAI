/* CREATE table mock_data(
id SERIAL PRIMARY KEY,
customer_first_name VARCHAR(50),
customer_last_name VARCHAR(50),
customer_age INTEGER,
customer_email VARCHAR(100),
customer_country VARCHAR(100),
customer_postal_code VARCHAR(50),
customer_pet_type VARCHAR(20),
customer_pet_name VARCHAR(100),
customer_pet_breed VARCHAR(100),
seller_first_name VARCHAR(50),
seller_last_name VARCHAR(50),
seller_email VARCHAR(100),
seller_country VARCHAR(100),
seller_postal_code VARCHAR(50),
product_name VARCHAR(100),
product_category VARCHAR(50),
product_price NUMERIC(10,2),
product_quantity INTEGER,
sale_date DATE,
sale_customer_id INTEGER REFERENCES dim --заполнить для них данные из сырой базы
sale_seller_id INTEGER REFERENCES dim
sale_product_id INTEGER REFERENCES dim
sale_quantity INTEGER,
sale_total_price NUMERIC(10,2),
store_name VARCHAR(100),
store_location VARCHAR(200),
store_city VARCHAR(50),
store_state VARCHAR(10),
store_country VARCHAR(100),
store_phone VARCHAR(20),
store_email VARCHAR(100),
pet_category VARCHAR(50),
product_weight NUMERIC(6,2),
product_color VARCHAR(20),
product_size VARCHAR(20),
product_brand VARCHAR(50),
product_material VARCHAR(50),
product_description TEXT,
product_rating NUMERIC(3,1),
product_reviews INTEGER,
product_release_date DATE,
product_expiry_date DATE,
supplier_name VARCHAR(100),
supplier_contact VARCHAR(50),
supplier_email VARCHAR(100),
supplier_phone VARCHAR(20),
supplier_address VARCHAR(50),
supplier_city VARCHAR(50),
supplier_country VARCHAR(100)
); */
CREATE table
    customer (
        customer_id SERIAL PRIMARY KEY,
        customer_first_name VARCHAR(50),
        customer_last_name VARCHAR(50),
        customer_age INTEGER,
        customer_email VARCHAR(100),
        customer_country VARCHAR(100),
        customer_postal_code VARCHAR(50),
        customer_pet_type VARCHAR(20),
        customer_pet_name VARCHAR(100),
        customer_pet_breed VARCHAR(100),
        pet_category VARCHAR(50) --Дублируем так как нужно здесь тоже
    );

CREATE table
    seller (
        seller_id SERIAL PRIMARY KEY,
        seller_first_name VARCHAR(50),
        seller_last_name VARCHAR(50),
        seller_email VARCHAR(100),
        seller_country VARCHAR(100),
        seller_postal_code VARCHAR(50)
    );

CREATE table
    product (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(100),
        product_category VARCHAR(50),
        product_price NUMERIC(10, 2),
        product_quantity INTEGER,
        product_weight NUMERIC(6, 2),
        product_color VARCHAR(20),
        product_size VARCHAR(20),
        product_brand VARCHAR(50),
        product_material VARCHAR(50),
        product_description TEXT,
        product_rating NUMERIC(3, 1),
        product_reviews INTEGER,
        pet_category VARCHAR(50), --Дублируем так как нужно здесь тоже
        product_release_date DATE,
        product_expiry_date DATE
        
    );

CREATE table
    sale (
        sale_id SERIAL PRIMARY KEY,
        sale_date DATE,
        sale_customer_id INTEGER REFERENCES customer (customer_id),
        sale_seller_id INTEGER REFERENCES seller (seller_id),
        sale_product_id INTEGER REFERENCES product (product_id),
        sale_quantity INTEGER,
        sale_total_price NUMERIC(10, 2)
    );

CREATE table
    store (
        store_id SERIAL PRIMARY KEY,
        store_name VARCHAR(100),
        store_location VARCHAR(200),
        store_city VARCHAR(50),
        store_state VARCHAR(10),
        store_country VARCHAR(100),
        store_phone VARCHAR(20),
        store_email VARCHAR(100)
    );

CREATE table
    seller_store (
        PRIMARY KEY (seller_id, store_id),
        seller_id INTEGER REFERENCES seller (seller_id),
        store_id INTEGER REFERENCES store (store_id)
    );

CREATE table
    supplier (
        supplier_id SERIAL PRIMARY KEY,
        supplier_name VARCHAR(100),
        supplier_contact VARCHAR(50),
        supplier_email VARCHAR(100),
        supplier_phone VARCHAR(20),
        supplier_address VARCHAR(50),
        supplier_city VARCHAR(50),
        supplier_country VARCHAR(100)
    );

CREATE table
    supplier_store (
        PRIMARY KEY (supplier_id, store_id),
        supplier_id INTEGER REFERENCES supplier (supplier_id),
        store_id INTEGER REFERENCES store (store_id)
    );