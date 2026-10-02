"""Consolidated bronze source schemas for all OLTP entities.

Each schema defines the expected CSV columns as all-StringType StructFields,
preserving raw source values faithfully for the bronze layer.

Usage:
    from configs.bronze_schemas import SCHEMAS
    customer_schema = SCHEMAS["customer"]
"""

from pyspark.sql.types import StructType, StructField, StringType


SCHEMAS = {
    "address": StructType([
        StructField("source_record_id", StringType(), True),
        StructField("address_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("address_line_1", StringType(), True),
        StructField("address_line_2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_region", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("is_primary", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]),

    "category": StructType([
        StructField("source_record_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("parent_category_id", StringType(), True),
        StructField("category_name", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("is_active", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]),

    "customer": StructType([
        StructField("source_record_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("loyalty_status", StringType(), True),
        StructField("marketing_opt_in", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]),

    "order": StructType([
        StructField("source_record_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("billing_address_id", StringType(), True),
        StructField("shipping_address_id", StringType(), True),
        StructField("order_timestamp", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("subtotal_amount", StringType(), True),
        StructField("discount_amount", StringType(), True),
        StructField("tax_amount", StringType(), True),
        StructField("shipping_amount", StringType(), True),
        StructField("order_total_amount", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]),

    "order_item": StructType([
        StructField("source_record_id", StringType(), True),
        StructField("order_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("discount_amount", StringType(), True),
        StructField("tax_amount", StringType(), True),
        StructField("line_total_amount", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]),

    "payment": StructType([
        StructField("source_record_id", StringType(), True),
        StructField("payment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("payment_timestamp", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("transaction_reference", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]),

    "product": StructType([
        StructField("source_record_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("is_active", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]),

    "product_category": StructType([
        StructField("source_record_id", StringType(), True),
        StructField("product_category_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("is_primary", StringType(), True),
        StructField("active_from", StringType(), True),
        StructField("active_to", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]),

    "return": StructType([
        StructField("source_record_id", StringType(), True),
        StructField("return_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("return_timestamp", StringType(), True),
        StructField("return_status", StringType(), True),
        StructField("return_reason", StringType(), True),
        StructField("total_refund_amount", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]),

    "return_item": StructType([
        StructField("source_record_id", StringType(), True),
        StructField("return_item_id", StringType(), True),
        StructField("return_id", StringType(), True),
        StructField("order_item_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("refund_amount", StringType(), True),
        StructField("return_reason", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]),
}
