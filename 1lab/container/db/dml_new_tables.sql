TRUNCATE TABLE supplier_store CASCADE;
TRUNCATE TABLE supplier CASCADE;
TRUNCATE TABLE seller_store CASCADE;
TRUNCATE TABLE store CASCADE;
TRUNCATE TABLE sale CASCADE;
TRUNCATE TABLE product CASCADE;
TRUNCATE TABLE seller CASCADE;
TRUNCATE TABLE customer CASCADE;


INSERT INTO
    customer (
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_age,
        customer_email,
        customer_country,
        customer_postal_code,
        customer_pet_type,
        customer_pet_name,
        customer_pet_breed,
        pet_category --Дублируем так как нужно здесь тоже
    )
    SELECT DISTINCT ON (sale_customer_id)
        sale_customer_id::INTEGER,
        customer_first_name::VARCHAR(50),
        customer_last_name::VARCHAR(50),
        customer_age::INTEGER,
        customer_email::VARCHAR(100),
        customer_country::VARCHAR(100),
        customer_postal_code::VARCHAR(50),
        customer_pet_type::VARCHAR(20),
        customer_pet_name::VARCHAR(100),
        customer_pet_breed::VARCHAR(100),
        pet_category::VARCHAR(50) --Дублируем так как нужно здесь тоже
    FROM mock_data
    WHERE sale_customer_id IS NOT NULL
    ON CONFLICT DO NOTHING;

INSERT INTO
    seller (
        seller_id,
        seller_first_name,
        seller_last_name,
        seller_email,
        seller_country,
        seller_postal_code
    )
    SELECT DISTINCT ON (sale_seller_id)
        sale_seller_id::INTEGER,
        seller_first_name::VARCHAR(50),
        seller_last_name::VARCHAR(50),
        seller_email::VARCHAR(100),
        seller_country::VARCHAR(100),
        seller_postal_code::VARCHAR(50)
    FROM mock_data
    WHERE sale_seller_id IS NOT NULL
    ON CONFLICT DO NOTHING;

INSERT INTO
    product (
        product_id,
        product_name,
        product_category,
        product_price,
        product_quantity,
        product_weight,
        product_color,
        product_size,
        product_brand,
        product_material,
        product_description,
        product_rating,
        product_reviews,
        pet_category, --Дублируем так как нужно здесь тоже
        product_release_date,
        product_expiry_date
    )
    SELECT DISTINCT ON (sale_seller_id)
        sale_product_id::INTEGER,
        product_name::VARCHAR(100),
        product_category::VARCHAR(50),
        product_price::NUMERIC(10, 2),
        product_quantity::INTEGER,
        product_weight::NUMERIC(6, 2),
        product_color::VARCHAR(20),
        product_size::VARCHAR(20),
        product_brand::VARCHAR(50),
        product_material::VARCHAR(50),
        product_description::TEXT,
        product_rating::NUMERIC(3, 1),
        product_reviews::INTEGER,
        pet_category::VARCHAR(50), --Дублируем так как нужно здесь тоже
        TO_DATE(product_release_date, 'MM/DD/YYYY'),
        TO_DATE(product_expiry_date, 'MM/DD/YYYY')
    FROM mock_data
    WHERE sale_product_id IS NOT NULL
    ON CONFLICT DO NOTHING;

INSERT INTO
    sale (
        sale_date,
        sale_customer_id,
        sale_seller_id,
        sale_product_id,
        sale_quantity,
        sale_total_price
    )
    SELECT
        TO_DATE(sale_date, 'MM/DD/YYYY'),
        sale_customer_id::INTEGER,
        sale_seller_id::INTEGER,
        sale_product_id::INTEGER,
        sale_quantity::INTEGER,
        sale_total_price::NUMERIC(10, 2)
    FROM mock_data;

INSERT INTO
    store (
        store_name,
        store_location,
        store_city,
        store_state,
        store_country,
        store_phone,
        store_email
    )
    SELECT DISTINCT
        store_name::VARCHAR(100),
        store_location::VARCHAR(200),
        store_city::VARCHAR(50),
        store_state::VARCHAR(10),
        store_country::VARCHAR(100),
        store_phone::VARCHAR(20),
        store_email::VARCHAR(100)
    FROM mock_data;

INSERT INTO seller_store (seller_id, store_id)
SELECT DISTINCT
    m.sale_seller_id::INTEGER,
    st.store_id
FROM mock_data m
JOIN store st ON st.store_phone = m.store_phone
WHERE m.sale_seller_id IS NOT NULL
ON CONFLICT (seller_id, store_id) DO NOTHING;

INSERT INTO
    supplier (
        supplier_name,
        supplier_contact,
        supplier_email,
        supplier_phone,
        supplier_address,
        supplier_city,
        supplier_country
    )
    SELECT DISTINCT
        supplier_name::VARCHAR(100),
        supplier_contact::VARCHAR(50),
        supplier_email::VARCHAR(100),
        supplier_phone::VARCHAR(20),
        supplier_address::VARCHAR(50),
        supplier_city::VARCHAR(50),
        supplier_country::VARCHAR(100)
    FROM mock_data;



-- Индексы на store
CREATE INDEX IF NOT EXISTS idx_store_name ON store(store_name);

-- Индексы на supplier
CREATE INDEX IF NOT EXISTS idx_supplier_name ON supplier(supplier_name);

-- Индекс на mock_data для ускорения
CREATE INDEX IF NOT EXISTS idx_mock_data_supplier_store 
ON mock_data (supplier_name, store_name);




INSERT INTO supplier_store (supplier_id, store_id)
SELECT DISTINCT
    sup.supplier_id,
    st.store_id
FROM (
    SELECT DISTINCT 
        supplier_phone, 
        store_phone 
    FROM mock_data 
    WHERE supplier_phone IS NOT NULL 
      AND store_phone IS NOT NULL
) m
JOIN store st ON st.store_phone = m.store_phone
JOIN supplier sup ON sup.supplier_phone = m.supplier_phone
ON CONFLICT (supplier_id, store_id) DO NOTHING;
