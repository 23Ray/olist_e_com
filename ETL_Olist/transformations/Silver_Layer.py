from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

import dlt

# Customers Silver Layer

dlt.create_streaming_table(name = 'olist_db.silver.customers')

dlt.create_auto_cdc_flow(
    target = 'olist_db.silver.customers',
    source = 'olist_db.landing.customers',
    keys = ['customer_id'],
    sequence_by = col("ETL_Date"),
    stored_as_scd_type = 2
)

# Geolocation mers Silver Layer

dlt.create_streaming_table(name = 'olist_db.silver.geolocation')

dlt.create_auto_cdc_flow(
    target = 'olist_db.silver.geolocation',
    source = 'olist_db.landing.geolocation',
    keys = ['geolocation_zip_code_prefix'],
    sequence_by = col("ETL_Date"),
    stored_as_scd_type = 2
)

# Order_Items mers Silver Layer

dlt.create_streaming_table(name = 'olist_db.silver.order_items')

dlt.create_auto_cdc_flow(
    target = 'olist_db.silver.order_items',
    source = 'olist_db.landing.order_items',
    keys = ['order_id'],
    sequence_by = col("ETL_Date"),
    stored_as_scd_type = 2
)

# Order_Payments mers Silver Layer

dlt.create_streaming_table(name = 'olist_db.silver.order_payments')

dlt.create_auto_cdc_flow(
    target = 'olist_db.silver.order_payments',
    source = 'olist_db.landing.order_payments',
    keys = ['order_id'],
    sequence_by = col("ETL_Date"),
    stored_as_scd_type = 2
)

# order_reviews Silver Layer

dlt.create_streaming_table(name = 'olist_db.silver.order_reviews')

dlt.create_auto_cdc_flow(
    target = 'olist_db.silver.order_reviews',
    source = 'olist_db.landing.order_reviews',
    keys = ['review_id'],
    sequence_by = col("ETL_Date"),
    stored_as_scd_type = 2
)

# orders Silver Layer

dlt.create_streaming_table(name = 'olist_db.silver.orders')

dlt.create_auto_cdc_flow(
    target = 'olist_db.silver.orders',
    source = 'olist_db.landing.orders',
    keys = ['order_id'],
    sequence_by = col("ETL_Date"),
    stored_as_scd_type = 2
)

# products Silver Layer

dlt.create_streaming_table(name = 'olist_db.silver.products')

dlt.create_auto_cdc_flow(
    target = 'olist_db.silver.products',
    source = 'olist_db.landing.products',
    keys = ['product_id'],
    sequence_by = col("ETL_Date"),
    stored_as_scd_type = 2
)

# sellers Silver Layer

dlt.create_streaming_table(name = 'olist_db.silver.sellers')

dlt.create_auto_cdc_flow(
    target = 'olist_db.silver.sellers',
    source = 'olist_db.landing.sellers',
    keys = ['seller_id'],
    sequence_by = col("ETL_Date"),
    stored_as_scd_type = 2
)

# product_category_name_translation Silver Layer

dlt.create_streaming_table(name = 'olist_db.silver.product_category_name_translation')

dlt.create_auto_cdc_flow(
    target = 'olist_db.silver.product_category_name_translation',
    source = 'olist_db.landing.product_category_name_translation',
    keys = ['product_category_name', 'product_category_name_english'],
    sequence_by = col("ETL_Date"),
    stored_as_scd_type = 2
)