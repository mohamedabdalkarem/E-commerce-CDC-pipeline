#!/bin/bash
set -e
set -x   

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<EOF

-- Reference / lookup tables
COPY product_category_name_translation
FROM '/data/product_category_name_translation.csv'
DELIMITER ',' CSV HEADER NULL '';

COPY sellers
FROM '/data/sellers.csv'
DELIMITER ',' CSV HEADER NULL '';

COPY customers
FROM '/data/customers.csv'
DELIMITER ',' CSV HEADER NULL '';

COPY geolocation
FROM '/data/geolocation.csv'
DELIMITER ',' CSV HEADER NULL '';

COPY products
FROM '/data/products.csv'
DELIMITER ',' CSV HEADER NULL '';

COPY orders
FROM '/data/orders.csv'
DELIMITER ',' CSV HEADER NULL '';

COPY order_items
FROM '/data/order_items.csv'
DELIMITER ',' CSV HEADER NULL '';

COPY order_payments
FROM '/data/order_payments.csv'
DELIMITER ',' CSV HEADER NULL '';

COPY order_reviews
FROM '/data/order_reviews.csv'
DELIMITER ',' CSV HEADER NULL '';

COPY leads_qualified
FROM '/data/leads_qualified.csv'
DELIMITER ',' CSV HEADER NULL '';

COPY leads_closed
FROM '/data/leads_closed.csv'
DELIMITER ',' CSV HEADER NULL '';

EOF

echo "All data loaded successfully!"