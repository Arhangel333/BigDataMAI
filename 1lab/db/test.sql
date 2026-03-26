\set field product_expiry_date

select DISTINCT 
COUNT(:field) as count_of_words,
LENGTH(:field) as len
FROM mock_data
GROUP BY LENGTH(:field)
ORDER BY len DESC 
LIMIT 10;


select DISTINCT
:field,
LENGTH(:field) as len 
FROM mock_data 
ORDER BY len DESC
LIMIT 10;


select DISTINCT
:field,
LENGTH(:field) as len 
FROM mock_data 
ORDER BY len ASC
LIMIT 10;

select DISTINCT
id,
supplier_name,
store_name, 
seller_first_name
FROM mock_data 
WHERE seller_first_name IS NULL
ORDER BY id ASC
LIMIT 10;

\dt

SELECT 
COUNT(*) FROM (SELECT DISTINCT 
        supplier_name, 
        store_name 
    FROM mock_data 
    WHERE supplier_name IS NOT NULL 
      AND store_name IS NOT NULL);




/* 
-- Найти все поля, которые отличаются для одного магазина
SELECT 
    store_name,
    COUNT(DISTINCT store_location) as location_variants,
    COUNT(DISTINCT store_city) as city_variants,
    COUNT(DISTINCT store_state) as state_variants,
    COUNT(DISTINCT store_country) as country_variants,
    COUNT(DISTINCT store_phone) as phone_variants,
    COUNT(DISTINCT store_email) as email_variants,
    COUNT(*) as total_rows
FROM mock_data
WHERE store_name IS NOT NULL
GROUP BY store_name
HAVING 
    COUNT(DISTINCT store_location) > 1 
    OR COUNT(DISTINCT store_city) > 1 
    OR COUNT(DISTINCT store_state) > 1 
    OR COUNT(DISTINCT store_country) > 1 
    OR COUNT(DISTINCT store_phone) > 1 
    OR COUNT(DISTINCT store_email) > 1
ORDER BY total_rows DESC; */

SELECT 
COUNT(id) FROM mock_data;  

SELECT 
COUNT(customer_id) FROM customer;      
     
SELECT 
COUNT(product_id) FROM product;        
SELECT 
COUNT(sale_id) FROM sale;           
SELECT 
COUNT(seller_id) FROM seller;          
 SELECT 
COUNT(store_name) FROM store;          
 SELECT 
COUNT(supplier_id) FROM supplier; 

 SELECT 
COUNT(DISTINCT (supplier_name, store_name)) FROM mock_data;  
/* 
EXPLAIN (ANALYZE, BUFFERS, TIMING)
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
JOIN supplier sup ON sup.supplier_phone = m.supplier_phone; */



SELECT 
    'customer' as table_name, COUNT(*) FROM customer
UNION ALL SELECT 'seller', COUNT(*) FROM seller
UNION ALL SELECT 'product', COUNT(*) FROM product
UNION ALL SELECT 'store', COUNT(*) FROM store
UNION ALL SELECT 'sale', COUNT(*) FROM sale
UNION ALL SELECT 'supplier', COUNT(*) FROM supplier
UNION ALL SELECT 'seller_store', COUNT(*) FROM seller_store
UNION ALL SELECT 'supplier_store', COUNT(*) FROM supplier_store;

/* EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT DISTINCT
    m.sale_seller_id::INTEGER,
    st.store_id
FROM mock_data m
JOIN store st ON st.store_phone = m.store_phone
WHERE m.sale_seller_id IS NOT NULL; */