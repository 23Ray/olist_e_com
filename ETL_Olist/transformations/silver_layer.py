from pyspark.sql.functions import *
import dlt

spark.conf.set(
  "spark.databricks.delta.schema.autoMerge.enabled", "false"
)

from configs.silver_metadata import silver_metadata


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

