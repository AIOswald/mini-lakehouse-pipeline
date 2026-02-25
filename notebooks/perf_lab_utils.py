import time
from typing import Optional, List, Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def perf_lab(
    df: DataFrame,
    name: str,
    *,
    action: str = "count",                # "count" | "take" | "none"
    take_n: int = 5,
    cache: bool = False,
    explain: bool = False,                # formatted logical/physical plan
    partitions: bool = True,              # show partitions
    keys: Optional[List[str]] = None,     # distinct check
    null_cols: Optional[List[str]] = None,  # null check
    negative_cols: Optional[List[str]] = None,  # check <0 (for qty/price)
    sample: bool = False,                 # show small sample
    notes: Optional[str] = None,
    emit_view: Optional[str] = None,
    max_nulls: int = 0,
    max_negatives: int = 0,
    require_distinct_keys: bool = True,
    max_time_s: Optional[float] = None,
) -> Dict[str, Any]:
    """
    perf_lab: misst Laufzeit, Rowcount, und einfache Data Quality Checks.
    Gibt ein Dict mit Kennzahlen zurück (praktisch fürs Logging/Reporting).
    """
    metrics: Dict[str, Any] = {"name": name}
    issues: List[str] = []

    if notes:
        print(f"\nperf_lab: {name} - {notes}")

    if partitions:
        try:
            p = df.rdd.getNumPartitions()
            metrics["partitions"] = p
            print(f"Partitions: {p}")
        except Exception as e:
            print(f"Partitions: n/a ({e})")

    if explain:
        print("\n--- EXPLAIN (formatted) ---")
        df.explain(mode="formatted")

    if cache:
        df = df.cache()

    t0 = time.time()

    rowcount = None
    taken = None
    if action == "count":
        rowcount = df.count()
    elif action == "take":
        taken = df.take(take_n)
    elif action == "none":
        pass
    else:
        raise ValueError("action must be 'count', 'take', or 'none'")

    dt = time.time() - t0

    metrics["action"] = action
    metrics["time_s"] = round(dt, 4)
    if rowcount is not None:
        metrics["rows"] = rowcount
    if taken is not None:
        metrics["taken"] = taken

    print(
        f"Action: {action} | Time: {dt:.3f}s"
        + (f" | Rows: {rowcount}" if rowcount is not None else "")
    )
    if max_time_s is not None and dt > max_time_s:
        issues.append(f"time_s>{max_time_s}")

    if sample:
        try:
            display(df.limit(10))
        except Exception:
            print("Sample: display() not available here, use df.show(10, truncate=False)")
            df.show(10, truncate=False)

    # Distinct check
    if keys:
        existing_keys = [k for k in keys if k in df.columns]
        if existing_keys:
            t1 = time.time()
            distinct_cnt = (
                df.select(*[F.col(k) for k in existing_keys]).dropDuplicates().count()
            )
            dt1 = time.time() - t1
            metrics["distinct_keys"] = {
                "cols": existing_keys,
                "count": distinct_cnt,
                "time_s": round(dt1, 4),
            }
            print(f"Distinct({existing_keys}): {distinct_cnt} (took {dt1:.3f}s)")
            if require_distinct_keys and rowcount is not None and distinct_cnt != rowcount:
                issues.append("distinct_keys!=rows")
        else:
            print(f"Distinct: none of keys exist: {keys}")

    # Null check
    if null_cols:
        existing = [c for c in null_cols if c in df.columns]
        if existing:
            t2 = time.time()
            agg_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in existing]
            nulls = df.agg(*agg_exprs).collect()[0].asDict()
            dt2 = time.time() - t2
            metrics["nulls"] = {
                "cols": existing,
                "counts": nulls,
                "time_s": round(dt2, 4),
            }
            print(f"Nulls (took {dt2:.3f}s): {nulls}")
            if any(v > max_nulls for v in nulls.values()):
                issues.append("nulls>max")
        else:
            print(f"Null check: none of cols exist: {null_cols}")

    # Negative check
    if negative_cols:
        existing = [c for c in negative_cols if c in df.columns]
        if existing:
            t3 = time.time()
            neg_exprs = [F.sum((F.col(c) < 0).cast("int")).alias(c) for c in existing]
            negs = df.agg(*neg_exprs).collect()[0].asDict()
            dt3 = time.time() - t3
            metrics["negatives"] = {
                "cols": existing,
                "counts": negs,
                "time_s": round(dt3, 4),
            }
            print(f"Negatives (took {dt3:.3f}s): {negs}")
            if any(v > max_negatives for v in negs.values()):
                issues.append("negatives>max")
        else:
            print(f"Negative check: none of cols exist: {negative_cols}")

    status = "OK" if not issues else "WARN"
    metrics["status"] = status
    metrics["issues"] = issues
    print(f"Status: {status}" + (f" | Issues: {issues}" if issues else ""))

    if emit_view:
        _append_metrics_to_view(metrics, emit_view)

    return metrics

def _append_metrics_to_view(metrics: Dict[str, Any], view_name: str) -> None:
    import json
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    if spark is None:
        return
    # Flatten complex types to JSON strings to avoid schema inference conflicts
    flat = {}
    for k, v in metrics.items():
        if isinstance(v, (dict, list, tuple)):
            flat[k] = json.dumps(v, ensure_ascii=False)
        else:
            flat[k] = v
    df = spark.createDataFrame([flat])
    v = view_name
    if v.startswith("global_temp."):
        vname = v.split(".", 1)[1]
        full = v
    else:
        vname = v
        full = f"global_temp.{v}"
    try:
        existing = spark.table(full)
        df = existing.unionByName(df, allowMissingColumns=True)
    except Exception:
        pass
    df.createOrReplaceGlobalTempView(vname)



def perf_delta(before: int, after: int, label: str) -> Dict[str, Any]:
    dropped = before - after
    pct = (dropped / before * 100) if before else 0.0
    msg = f"{label}: before={before}, after={after}, dropped={dropped} ({pct:.2f}%)"
    print(msg)
    return {
        "label": label,
        "before": before,
        "after": after,
        "dropped": dropped,
        "dropped_pct": round(pct, 4),
    }
