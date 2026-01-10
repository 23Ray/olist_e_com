from pyspark.sql.functions import *
from pyspark.sql.types import *

import dlt

# Customers Landing Layer

customers_expectations = {
    "customer_id_not_null": "customer_id is not null",
    "customer_unique_id_not_null": "customer_unique_id is not null"
}

@dlt.table(
    name = 'olist_db.dlt.customers'
)
@dlt.expect_all_or_fail(customers_expectations)
def customers_dlt():
    df = spark.readStream.table("olist_db.bronze.customers")
    df = df.select(
        col('customer_id').cast("string"),
        col('customer_unique_id').cast("string"),
        col('customer_zip_code_prefix').cast("int"),
        col('customer_city').cast("string"),
        col('customer_state').cast("string"),
        col('ETL_Date')
    )
    df = df.filter(col("customer_zip_code_prefix").isNotNull()).filter(col("customer_city").isNotNull()).filter(col("customer_state").isNotNull())
    return df

@dlt.table(
    name = 'olist_db.landing.customers'
)
def customers_landing():
    df = dlt.read_stream('olist_db.dlt.customers')
    df = df.dropDuplicates(['customer_id'])
    return df

@dlt.table(name="olist_db.landing.customers_quarantine")
def bad_customers():
    df = spark.readStream.table("olist_db.bronze.customers").filter(
        col("customer_zip_code_prefix").isNull() |
        col("customer_city").isNull() |
        col("customer_state").isNull()
    )
    return df

# Geolocation Landing Layer

geolocation_expectations = {
    "geolocation_zip_code_prefix_not_null": "geolocation_zip_code_prefix is not null"
}

@dlt.table(
    name = 'olist_db.dlt.geolocation'
)
@dlt.expect_all_or_fail(geolocation_expectations)
def geolocation_dlt():
    df = spark.readStream.table("olist_db.bronze.geolocation")
    df = df.select(
        col('geolocation_zip_code_prefix').cast('int'),
        col('geolocation_lat').cast('double'),
        col('geolocation_lng').cast('double'),
        col('geolocation_city').cast('string'),
        col('geolocation_state').cast('string'),
        col('ETL_Date')
    )
    return df

@dlt.table(
    name = 'olist_db.landing.geolocation'
)
def geolocation_landing():
    df = dlt.read_stream('olist_db.dlt.geolocation')
    df = df.dropDuplicates(['geolocation_zip_code_prefix'])
    return df

# Order_Items Landing Layer

order_items_expectations = {
    "order_id_not_null": "order_id is not null"
}

@dlt.table(
    name = 'olist_db.dlt.order_items'
)
@dlt.expect_all_or_fail(order_items_expectations)
def order_items_dlt():
    df = spark.readStream.table("olist_db.bronze.order_items")
    df = df.select(
        col('order_id').cast('string'),
        col('order_item_id').cast('int'),
        col('product_id').cast('string'),
        col('seller_id').cast('string'),
        col('shipping_limit_date').cast('timestamp'),
        col('price').cast('double'),
        col('freight_value').cast('double'),
        col('ETL_Date')
    )
    return df

@dlt.table(
    name = 'olist_db.landing.order_items'
)
def order_items_landing():
    df = dlt.read_stream('olist_db.dlt.order_items')
    df = df.dropDuplicates(['order_id'])
    return df

# Order_Payments Landing Layer

order_payments_expectations = {
    "order_id_not_null": "order_id is not null"
}

@dlt.table(
    name = 'olist_db.dlt.order_payments'
)
@dlt.expect_all_or_fail(order_payments_expectations)
def order_payments_dlt():
    df = spark.readStream.table("olist_db.bronze.order_payments")
    df = df.select(
        col('order_id').cast('string'),
        col('payment_sequential').cast('int'),
        col('payment_type').cast('string'),
        col('payment_installments').cast('int'),
        col('payment_value').cast('double'),
        col('ETL_Date')
    )
    return df

@dlt.table(
    name = 'olist_db.landing.order_payments'
)
def order_payments_landing():
    df = dlt.read_stream('olist_db.dlt.order_payments')
    df = df.dropDuplicates(['order_id'])
    return df

# Order_Reviews Landing Layer

order_reviews_expectations = {
    "review_id_not_null": "review_id is not null"
}

@dlt.table(
    name = 'olist_db.dlt.order_reviews'
)
@dlt.expect_all_or_drop(order_reviews_expectations)
def order_reviews_dlt():
    df = spark.readStream.table("olist_db.bronze.order_reviews")
    df = df.select(
        col('review_id').cast('string'),
        col('order_id').cast('string'),
        col('review_score').cast('int'),
        col('review_comment_title').cast('string'),
        col('review_comment_message').cast('string'),
        col('review_creation_date').cast('timestamp'),
        col('review_answer_timestamp').cast('timestamp'),
        col('ETL_Date')
    )
    return df

@dlt.table(
    name = 'olist_db.landing.order_reviews'
)
def order_reviews_landing():
    df = dlt.read_stream('olist_db.dlt.order_reviews')
    df = df.dropDuplicates(['review_id'])
    return df

# Orders Landing Layer

orders_expectations = {
    "order_id_not_null": "order_id is not null"
}

@dlt.table(
    name = 'olist_db.dlt.orders'
)
@dlt.expect_all_or_drop(orders_expectations)
def orders_dlt():
    df = spark.readStream.table("olist_db.bronze.orders")
    df = df.select(
        col('order_id').cast('string'),
        col('customer_id').cast('string'),
        col('order_status').cast('string'),
        col('order_purchase_timestamp').cast('timestamp'),
        col('order_approved_at').cast('timestamp'),
        col('order_delivered_carrier_date').cast('timestamp'),
        col('order_delivered_customer_date').cast('timestamp'),
        col('order_estimated_delivery_date').cast('timestamp'),
        col('ETL_Date')
    )
    return df

@dlt.table(
    name = 'olist_db.landing.orders'
)
def orders_landing():
    df = dlt.read_stream('olist_db.dlt.orders')
    df = df.dropDuplicates(['order_id'])
    return df

# Products Landing Layer

products_expectations = {
    "product_id_not_null": "product_id is not null"
}

@dlt.table(
    name = 'olist_db.dlt.products'
)
@dlt.expect_all_or_drop(products_expectations)
def products_dlt():
    df = spark.readStream.table("olist_db.bronze.products")
    df = df.select(
        col('product_id').cast('string'),
        col('product_category_name').cast('string'),
        col('product_name_lenght').cast('double').alias('product_name_length'),
        col('product_description_lenght').cast('double').alias('product_description_length'),
        col('product_photos_qty').cast('int'),
        col('product_weight_g').cast('double'),
        col('product_length_cm').cast('double'),
        col('product_height_cm').cast('double'),
        col('product_width_cm').cast('double'),
        col('ETL_Date')
    )
    return df

@dlt.table(
    name = 'olist_db.landing.products'
)
def products_landing():
    df = dlt.read_stream('olist_db.dlt.products')
    df = df.dropDuplicates(['product_id'])
    return df

# Sellers Landing Layer

sellers_expectations = {
    "seller_id_not_null": "seller_id is not null"
}

@dlt.table(
    name = 'olist_db.dlt.sellers'
)
@dlt.expect_all_or_drop(sellers_expectations)
def sellers_dlt():
    df = spark.readStream.table("olist_db.bronze.sellers")
    df = df.select(
        col('seller_id').cast('string'),
        col('seller_zip_code_prefix').cast('int'),
        col('seller_city').cast('string'),
        col('seller_state').cast('string'),
        col('ETL_Date')
    )
    return df

@dlt.table(
    name = 'olist_db.landing.sellers'
)
def sellers_landing():
    df = dlt.read_stream('olist_db.dlt.sellers')
    df = df.dropDuplicates(['seller_id'])
    return df

# product_category_name_translation Landing Layer

@dlt.table(
    name = 'olist_db.dlt.product_category_name_translation'
)

def product_category_name_translation_dlt():
    df = spark.readStream.table("olist_db.bronze.product_category_name_translation")
    df = df.select(
        col('product_category_name').cast('string'),
        col('product_category_name_english').cast('string'),
        col('ETL_Date')
    )
    return df

@dlt.table(
    name = 'olist_db.landing.product_category_name_translation'
)
def product_category_name_translation_landing():
    df = dlt.read_stream('olist_db.dlt.product_category_name_translation')
    return df