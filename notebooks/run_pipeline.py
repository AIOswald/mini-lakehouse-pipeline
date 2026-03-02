%python
SHOW_VIEWS = False
SHOW_PERF_SUMMARY = True
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "mini_lakehouse"

def tn(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"

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

NOTEBOOK_ARGS = {"catalog": CATALOG, "schema": SCHEMA}
PERF_TABLE = tn("perf_metrics")
NOTEBOOK_TIMEOUT_S = 0  # 0 = kein Timeout

# Setup
dbutils.notebook.run("./00_setup_data", NOTEBOOK_TIMEOUT_S, NOTEBOOK_ARGS)
print("✓ Setup abgeschlossen")
print(f"Using UC target: {CATALOG}.{SCHEMA}")

# Bronze
dbutils.notebook.run("./01_ingest_bronze", NOTEBOOK_TIMEOUT_S, NOTEBOOK_ARGS)
print("✓ Bronze Layer erstellt")
show_view(tn("customers_bronze"))
show_view(tn("orders_bronze"))
show_view(tn("order_items_bronze"))

# Silver
dbutils.notebook.run("./02_clean_silver", NOTEBOOK_TIMEOUT_S, NOTEBOOK_ARGS)
print("✓ Silver Layer erstellt")

show_view(tn("customers_silver"))
show_view(tn("orders_silver"))
show_view(tn("order_items_silver"))

# Join
dbutils.notebook.run("./03_join_window", NOTEBOOK_TIMEOUT_S, NOTEBOOK_ARGS)
print("✓ Wide Layer erstellt")
show_view(tn("sales_wide_silver"))

# Gold
dbutils.notebook.run("./05_gold_kpis", NOTEBOOK_TIMEOUT_S, NOTEBOOK_ARGS)
print("✓ Gold KPIs erstellt")
show_view(tn("gold_kpis"))

# Perf summary
try:
    show_perf(PERF_TABLE, limit=100)
except Exception:
    print("Perf summary unavailable")
