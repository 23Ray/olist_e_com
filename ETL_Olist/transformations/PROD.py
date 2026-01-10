from pyspark.sql.functions import *
import dlt

spark.conf.set(
  "spark.databricks.delta.schema.autoMerge.enabled", "false"
)

silver_metadata = {
  "customers": {
    "source": "olist_db.bronze.customers",
    "layer": "dimension",
    "keys": ["customer_id"],
    "sequence_col": "etl_date",
    "scd_type": 2,
    "schema": {
      "customer_id": "string",
      "customer_unique_id": "string",
      "customer_zip_code_prefix": "string",
      "customer_city": "string",
      "customer_state": "string",
      "etl_date": "timestamp"
    },
    "expectations": {
      "valid_customer_id": "customer_id IS NOT NULL",
      "valid_state": "length(customer_state) = 2"
    },
    "quarantine": True
  },

  "orders": {
    "source": "olist_db.bronze.orders",
    "layer": "fact",
    "keys": ["order_id"],
    "scd_type": None,
    "schema": {
      "order_id": "string",
      "customer_id": "string",
      "order_status": "string",
      "order_purchase_timestamp": "timestamp",
      "order_delivered_customer_date": "timestamp",
      "etl_date": "timestamp"
    },
    "expectations": {
      "valid_order_id": "order_id IS NOT NULL",
      "valid_customer_id": "customer_id IS NOT NULL",
      "valid_order_status": "order_status IN ('approved','processing','shipped','delivered','canceled','unavailable')",
      "valid_purchase_time": "order_purchase_timestamp IS NOT NULL",
      "valid_delivery_time": "order_delivered_customer_date >= order_purchase_timestamp"
    },
    "quarantine": False
  },

    "order_items": {
    "source": "olist_db.bronze.order_items",
    "layer": "fact",
    "keys": ["order_item_id"],
    "scd_type": None,
    "schema": {
      "order_id": "string",
      "order_item_id": "int",
      "product_id": "string",
      "seller_id": "string",
      "price": "double",
      "freight_value": "double",
      "etl_date": "timestamp"
    },
    "expectations": {
      "valid_order_id": "order_item_id IS NOT NULL AND product_id IS NOT NULL AND seller_id IS NOT NULL",
      "valid_price": "price > 0",
      "valid_freight_value": "freight_value >= 0"
    },
    "quarantine": False
  },
    
    "products": {
    "source": "olist_db.bronze.products",
    "layer": "dimension",
    "keys": ["product_id"],
    "sequence_col": "etl_date",
    "scd_type": 2,
    "schema": {
      "product_id": "string",
      "product_category_name": "string",
      "product_weight_g": "double",
      "product_length_cm": "double",
      "product_height_cm": "double",
      "product_width_cm": "double",
      "etl_date": "timestamp"
    },
    "expectations": {
      "valid_product_id": "product_id IS NOT NULL",
      "valid_product_weight": "LEN(product_weight_g) > 0",
      "valid_product_length": "LEN(product_length_cm) > 0",
      "valid_product_height": "LEN(product_height_cm) > 0",
      "valid_product_width": "LEN(product_width_cm) > 0"
    },
    "quarantine": True
    },
     "sellers": {
    "source": "olist_db.bronze.sellers",
    "layer": "dimension",
    "keys": ["seller_id"],
    "sequence_col": "etl_date",
    "scd_type": 2,
    "schema": {
      "seller_id": "string",
      "seller_zip_code_prefix": "string",
      "seller_city": "string",
      "seller_state": "string",
      "etl_date": "timestamp"
    },
    "expectations": {
      "valid_seller_id": "seller_id IS NOT NULL",
      "valid_state": "length(seller_state) = 2"
    },
    "quarantine": True
    },
     "order_payments": {
    "source": "olist_db.bronze.order_payments",
    "layer": "fact",
    "keys": ["order_id "],
    "scd_type": None,
    "schema": {
      "order_id": "string",
      "payment_sequential": "int",
      "payment_type": "string",
      "payment_installments": "int",
      "payment_value": "double",
      "etl_date": "timestamp"
    },
    "expectations": {
      "valid_payments": "order_id IS NOT NULL AND payment_value >= 0",
      "valid_payment_type": "payment_type IN ('credit_card','boleto','voucher','debit_card')"
    },
    "quarantine": False
  },
    "order_reviews": {
    "source": "olist_db.bronze.order_reviews",
    "layer": "fact",
    "keys": ["review_id "],
    "scd_type": None,
    "schema": {
      "order_id": "string",
      "review_id": "string",
      "review_score": "int",
      "review_creation_date": "timestamp",
      "etl_date": "timestamp"
    },
    "expectations": {
      "valid_review_id": "review_id is not null AND order_id IS NOT NULL AND review_score BETWEEN 1 AND 5"
    },
    "quarantine": False
  },
    "geolocation": {
    "source": "olist_db.bronze.geolocation",
    "layer": "dimension",
    "keys": ["geolocation_zip_code_prefix"],
    "sequence_col": "etl_date",
    "scd_type": 2,
    "schema": {
      "geolocation_zip_code_prefix": "string",
      "geolocation_lat": "double",
      "geolocation_lng": "double",
      "geolocation_city": "string",
      "geolocation_state": "string",
      "etl_date": "timestamp"
    },
    "expectations": {
      "valid_geolocation_id": "geolocation_zip_code_prefix IS NOT NULL",
      "valid_geolocation_value": "geolocation_lat BETWEEN -90 AND 90 AND geolocation_lng BETWEEN -180 AND 180"
    },
    "quarantine": True
    },
    "product_category_name_translation": {
    "source": "olist_db.bronze.product_category_name_translation",
    "layer": "dimension",
    "keys": ["product_category_name"],
    "sequence_col": "etl_date",
    "scd_type": 2,
    "schema": {
      "product_category_name": "string",
      "product_category_name_english": "string",
      "etl_date": "timestamp"
    },
    "expectations": {
      "valid_category_id": "product_category_name IS NOT NULL",
      "valid_category_name": "product_category_name_english IS NOT NULL"
    },
    "quarantine": True
    }
}

def apply_schema(df, schema: dict):
    return df.select([col(c).cast(t).alias(c) for c, t in schema.items()])


def apply_expectations(expectations: dict):
    for rule_name, rule in expectations.items():
        dlt.expect_or_drop(rule_name, rule)


def build_silver_base(meta):
    df = dlt.read_stream(meta["source"])
    df = apply_schema(df, meta["schema"])
    apply_expectations(meta["expectations"])
    return df


def build_invalid_records(meta):
    rules = " AND ".join(meta["expectations"].values())
    return (
        apply_schema(
            dlt.read_stream(meta["source"]),
            meta["schema"]
        )
        .filter(f"NOT ({rules})")
    )

def create_landing_table(table, meta):

    @dlt.table(
        name=f"olist_db.landing.{table}",
        comment=f"Validated Landing table for {table}"
    )
    def landing():
        return build_silver_base(meta)

for table, meta in silver_metadata.items():
    create_landing_table(table, meta)



def create_quarantine_table(table, meta):

    @dlt.table(
        name=f"olist_db.landing.{table}_invalid",
        comment=f"Quarantine table for invalid {table} records"
    )
    def quarantine():
        return build_invalid_records(meta)

for table, meta in silver_metadata.items():
    if meta["quarantine"]:
        create_quarantine_table(table, meta)


for table, meta in silver_metadata.items():
    if meta["layer"] == "dimension" and meta["scd_type"] == 2:

        dlt.create_streaming_table(
            name=f"olist_db.silver.{table}",
            comment=f"SCD Type 2 table for {table}"
        )

        dlt.create_auto_cdc_flow(
            target=f"olist_db.silver.{table}",
            source=f"olist_db.landing.{table}",
            keys=meta["keys"],
            sequence_by=meta["sequence_col"],
            stored_as_scd_type=2
        )

def create_fact_table(table):

    @dlt.table(
        name=f"olist_db.silver.{table}",
        comment=f"Cleaned fact table for {table}"
    )
    def fact():
        return dlt.read_stream(f"olist_db.landing.{table}")

for table, meta in silver_metadata.items():
    if meta["layer"] == "fact":
        create_fact_table(table)

