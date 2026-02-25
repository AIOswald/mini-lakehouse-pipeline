SHOW_VIEWS = False
SHOW_PERF_SUMMARY = True

def show_view(name: str, limit: int = 10):
    if not SHOW_VIEWS:
        return
    print(f"\n{name}:")
    display(spark.sql(f"SELECT * FROM {name} LIMIT {limit}"))

def show_perf(name: str, limit: int = 10):
    if not SHOW_PERF_SUMMARY:
        return
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

show_view("global_temp.customers_silver")
show_view("global_temp.orders_silver")
show_view("global_temp.order_items_silver")

# Join
dbutils.notebook.run("./03_join_window", 120)
print("✓ Wide Layer erstellt")
show_view("global_temp.sales_wide_silver")

# Perf summary
show_perf("global_temp.perf_metrics", limit=100)
