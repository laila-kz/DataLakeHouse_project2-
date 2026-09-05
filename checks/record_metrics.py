#!/usr/bin/env python3
"""
Platform Observability Metrics Tracker
Captures run-level operational telemetry, throughput, watermark lag, and data health.
"""

import os
import sys
import uuid
import json
import logging
from datetime import datetime
import duckdb

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("platform_metrics")


def get_duckdb_connection():
    db_path = os.environ.get("DUCKDB_PATH", os.path.join(os.getcwd(), "dbt", "dev.duckdb"))
    return duckdb.connect(db_path if os.path.exists(db_path) else ":memory:")


def init_metrics_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_metrics (
            batch_id VARCHAR,
            run_timestamp TIMESTAMP,
            layer VARCHAR,
            input_rows BIGINT,
            output_rows BIGINT,
            quarantined_rows BIGINT,
            duplicate_rate_pct DOUBLE,
            null_rate_pct DOUBLE,
            duration_sec DOUBLE,
            throughput_rows_sec DOUBLE,
            watermark_lag_sec BIGINT,
            status VARCHAR
        )
    """)


def record_run_metrics(
    batch_id=None,
    layer="end_to_end",
    input_rows=0,
    output_rows=0,
    quarantined_rows=0,
    duplicate_rate_pct=0.0,
    null_rate_pct=0.0,
    duration_sec=0.0,
    watermark_lag_sec=0,
    status="SUCCESS",
):
    batch_id = batch_id or str(uuid.uuid4())[:8]
    throughput = round(output_rows / duration_sec, 2) if duration_sec > 0 else output_rows
    now = datetime.utcnow()

    con = get_duckdb_connection()
    init_metrics_table(con)

    con.execute("""
        INSERT INTO pipeline_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        batch_id, now, layer, input_rows, output_rows, quarantined_rows,
        duplicate_rate_pct, null_rate_pct, duration_sec, throughput,
        watermark_lag_sec, status
    ))

    os.makedirs("audit", exist_ok=True)
    metric_payload = {
        "batch_id": batch_id,
        "timestamp": now.isoformat() + "Z",
        "layer": layer,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "quarantined_rows": quarantined_rows,
        "duplicate_rate_pct": duplicate_rate_pct,
        "null_rate_pct": null_rate_pct,
        "duration_sec": duration_sec,
        "throughput_rows_sec": throughput,
        "watermark_lag_sec": watermark_lag_sec,
        "status": status,
    }

    report_path = os.path.join("audit", f"metrics_{batch_id}.json")
    with open(report_path, "w") as f:
        json.dump(metric_payload, f, indent=2)

    logger.info(
        f"Pipeline Metrics Logged | Batch: {batch_id} | Rows: {output_rows} | "
        f"Throughput: {throughput} r/s | Lag: {watermark_lag_sec}s | Status: {status}"
    )
    return metric_payload


if __name__ == "__main__":
    record_run_metrics(
        batch_id=sys.argv[1] if len(sys.argv) > 1 else str(uuid.uuid4())[:8],
        layer="lakehouse_full",
        input_rows=10000,
        output_rows=9850,
        quarantined_rows=150,
        duplicate_rate_pct=0.02,
        null_rate_pct=0.01,
        duration_sec=14.2,
        watermark_lag_sec=120,
        status="SUCCESS",
    )
