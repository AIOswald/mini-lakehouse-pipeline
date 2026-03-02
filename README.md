# mini-lakehouse-pipeline

Databricks-only Demo-Pipeline fuer einen kleinen Lakehouse-Flow mit **Bronze -> Silver -> Gold** auf **Unity Catalog**.

## Ziel

Dieses Projekt zeigt einen einfachen End-to-End Ablauf in Databricks:
- Setup von Seed-Daten in Unity Catalog Tabellen
- Ingestion in Bronze
- Cleaning + Data Quality in Silver
- Join auf Wide Layer
- KPI-Berechnung in Gold
- Performance-/Quality-Metriken ueber `perf_lab`

## Voraussetzungen

- Databricks Workspace mit aktivem Unity Catalog
- Berechtigungen auf einen Catalog und ein Schema (default: `mini_lakehouse`)
- Laufender Databricks Cluster (Python/Spark)
- Repo in Databricks Repos eingecheckt

## Projektstruktur

- `notebooks/00_setup_data.ipynb`  
  Erstellt Seed-Daten und schreibt UC Tabellen (u.a. `customers_raw`, `orders_raw`, `order_items_raw`).
- `notebooks/01_ingest_bronze.ipynb`  
  Liest Raw-Tabellen, fuegt `ingest_ts` hinzu, schreibt Bronze-Tabellen.
- `notebooks/02_clean_silver.ipynb`  
  Typisierung, Deduplizierung, Checks, Referenzintegritaet, Silver-Tabellen.
- `notebooks/03_join_window.ipynb`  
  Baut `sales_wide_silver` (item-level wide table).
- `notebooks/05_gold_kpis.ipynb`  
  Aggregiert auf Order-Level und schreibt `gold_kpis`.
- `notebooks/run_pipeline.py`  
  Orchestriert alle Schritte in Reihenfolge.
- `notebooks/perf_lab_utils.py`  
  Utility fuer Laufzeit-/DQ-Metriken, schreibt nach `catalog.schema.perf_metrics`.

## Ausfuehrung in Databricks

1. Cluster starten.
2. Repo aktualisieren (Pull auf den gewuenschten Branch).
3. `notebooks/run_pipeline.py` oeffnen.
4. Notebook ausfuehren.

Die Pipeline verwendet standardmaessig:
- `CATALOG = current_catalog()`
- `SCHEMA = mini_lakehouse`

Anpassbar direkt in `run_pipeline.py`.

## Ergebnis-Tabellen (UC)

Im Ziel `catalog.schema` werden erzeugt/aktualisiert:
- `customers_raw`, `orders_raw`, `order_items_raw`
- `customers_bronze`, `orders_bronze`, `order_items_bronze`
- `customers_silver`, `orders_silver`, `order_items_silver`
- `sales_wide_silver`
- `gold_kpis`
- `perf_metrics`

## Hinweise

- Das Projekt ist fuer Databricks ausgelegt (kein lokaler Standalone-Run vorgesehen).
- Notebooks laufen explizit mit `%python`.
- Bei geaendertem Code in `perf_lab_utils.py` ggf. Cluster/Python neu starten, damit keine alten Modulstaende verwendet werden.
