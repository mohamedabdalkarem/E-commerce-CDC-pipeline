-- =========================
-- CUSTOMERS
-- =========================
CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
    customer_state TEXT
);

-- =========================
-- ORDERS
-- =========================
CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- =========================
-- ORDER ITEMS
-- =========================
CREATE TABLE order_items (
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TIMESTAMP,
    price DOUBLE PRECISION,
    freight_value DOUBLE PRECISION
);

-- =========================
-- ORDER PAYMENTS
-- =========================
CREATE TABLE order_payments (
    order_id TEXT,
    payment_sequential INTEGER,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value DOUBLE PRECISION
);

-- =========================
-- ORDER REVIEWS
-- =========================
CREATE TABLE order_reviews (
    review_id TEXT ,
    order_id TEXT,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- =========================
-- PRODUCTS
-- =========================
CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_name_lenght DOUBLE PRECISION,
    product_description_lenght DOUBLE PRECISION,
    product_photos_qty DOUBLE PRECISION,
    product_weight_g DOUBLE PRECISION,
    product_length_cm DOUBLE PRECISION,
    product_height_cm DOUBLE PRECISION,
    product_width_cm DOUBLE PRECISION
);

-- =========================
-- SELLERS
-- =========================
CREATE TABLE sellers (
    seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city TEXT,
    seller_state TEXT
);

-- =========================
-- GEOLOCATION
-- =========================
CREATE TABLE geolocation (
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat DOUBLE PRECISION,
    geolocation_lng DOUBLE PRECISION,
    geolocation_city TEXT,
    geolocation_state TEXT
);

-- =========================
-- CATEGORY TRANSLATION
-- =========================
CREATE TABLE product_category_name_translation (
    product_category_name TEXT ,
    product_category_name_english TEXT PRIMARY KEY
);

-- =========================
-- LEADS
-- =========================
CREATE TABLE leads_qualified (
    mql_id TEXT PRIMARY KEY,
    first_contact_date TIMESTAMP,
    landing_page_id TEXT,
    origin TEXT
);

CREATE TABLE leads_closed (
    mql_id TEXT PRIMARY KEY,
    seller_id TEXT,
    sdr_id TEXT,
    sr_id TEXT,
    won_date TIMESTAMP,
    business_segment TEXT,
    lead_type TEXT,
    lead_behaviour_profile TEXT,
    has_company  DOUBLE PRECISION,
    has_gtin  DOUBLE PRECISION,
    average_stock text,
    business_type TEXT,
    declared_product_catalog_size  DOUBLE PRECISION,
    declared_monthly_revenue DOUBLE PRECISION
);

-- =========================
-- 🔗 ADD RELATIONSHIPS SAFELY
-- =========================

ALTER TABLE orders
ADD CONSTRAINT fk_orders_customer
FOREIGN KEY (customer_id)
REFERENCES customers(customer_id)
NOT VALID;

ALTER TABLE order_items
ADD CONSTRAINT fk_order_items_order
FOREIGN KEY (order_id)
REFERENCES orders(order_id)
NOT VALID;

ALTER TABLE order_items
ADD CONSTRAINT fk_order_items_product
FOREIGN KEY (product_id)
REFERENCES products(product_id)
NOT VALID;

ALTER TABLE order_items
ADD CONSTRAINT fk_order_items_seller
FOREIGN KEY (seller_id)
REFERENCES sellers(seller_id)
NOT VALID;

ALTER TABLE order_payments
ADD CONSTRAINT fk_order_payments_order
FOREIGN KEY (order_id)
REFERENCES orders(order_id)
NOT VALID;

ALTER TABLE order_reviews
ADD CONSTRAINT fk_order_reviews_order
FOREIGN KEY (order_id)
REFERENCES orders(order_id)
NOT VALID;

ALTER TABLE leads_closed
ADD CONSTRAINT fk_leads_closed_mql
FOREIGN KEY (mql_id)
REFERENCES leads_qualified(mql_id)
NOT VALID;

ALTER TABLE leads_closed
ADD CONSTRAINT fk_leads_closed_seller
FOREIGN KEY (seller_id)
REFERENCES sellers(seller_id)
NOT VALID;