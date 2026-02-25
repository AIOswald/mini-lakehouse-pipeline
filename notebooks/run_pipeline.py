from perf_lab_utils import perf_lab

def show_view(name: str, limit: int = 10):
    print(f"\n{name}:")
    display(spark.sql(f"SELECT * FROM {name} LIMIT {limit}"))

# Setup
dbutils.notebook.run("./00_setup_data", 60)
print("✓ Setup abgeschlossen")

# Bronze
dbutils.notebook.run("./01_ingest_bronze", 60)
print("✓ Bronze Layer erstellt")
show_view("global_temp.customers_bronze_v")
show_view("global_temp.orders_bronze_v")
show_view("global_temp.order_items_bronze_v")

# Silver
dbutils.notebook.run("./02_clean_silver", 120)
print("✓ Silver Layer erstellt")
c = spark.table("global_temp.customers_silver")
o = spark.table("global_temp.orders_silver")
i = spark.table("global_temp.order_items_silver")

show_view("global_temp.customers_silver")
show_view("global_temp.orders_silver")
show_view("global_temp.order_items_silver")

perf_lab(c, "customers_silver", keys=["customer_id"], null_cols=["customer_id"])
perf_lab(o, "orders_silver", keys=["order_id"], null_cols=["order_id","customer_id"])
perf_lab(i, "order_items_silver", keys=["order_id","product_id"],
         null_cols=["order_id","product_id"], negative_cols=["quantity","unit_price"])

# Join
dbutils.notebook.run("./03_join_window", 120)
print("✓ Wide Layer erstellt")
show_view("global_temp.sales_wide_silver")
