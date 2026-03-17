"""
Airflow DAG: VNPT Money Data Pipeline
======================================
Schedule: weekly (every Monday at 02:00 UTC)

Tasks:
  generate_run_id  →  crawl  →  save_raw  →  clean
                                                    →  save_clean  →  load_neo4j  →  backup  →  notify

XCom is used to pass run_id and data keys between tasks.
Heavy data (JSON payload) is passed via MinIO keys, not XCom, to avoid
Airflow DB bloat.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict

# ---------------------------------------------------------------------------
# Airflow imports (graceful stub for environments without Airflow installed)
# ---------------------------------------------------------------------------

try:
    from airflow import DAG                                         # type: ignore
    from airflow.operators.python import PythonOperator            # type: ignore
    from airflow.operators.empty  import EmptyOperator             # type: ignore
    from airflow.utils.dates      import days_ago                   # type: ignore
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    logging.warning("apache-airflow not installed – DAG definition is for reference only")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _generate_run_id(**context) -> str:
    run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    logger.info("run_id = %s", run_id)
    context["ti"].xcom_push(key="run_id", value=run_id)
    return run_id


def _crawl(**context):
    run_id = context["ti"].xcom_pull(key="run_id")
    use_playwright = context["params"].get("use_playwright", False)

    from pipeline.crawlers.vnpt_scraper import VNPTScraper
    from pipeline.storage.minio_client import get_datalake

    scraper = VNPTScraper(use_playwright=use_playwright)
    data    = scraper.run()
    raw_html = scraper.get_raw_html() or ""

    lake = get_datalake()
    if lake._client:
        if raw_html:
            lake.save_raw_html(run_id, raw_html)
        raw_key = lake.save_raw_json(run_id, data)
        context["ti"].xcom_push(key="raw_key", value=raw_key)
        logger.info("Raw saved: %s", raw_key)
    else:
        # Fallback: push counts only (can't pass full JSON via XCom)
        context["ti"].xcom_push(key="raw_key", value="")
        logger.warning("MinIO unavailable – raw data not persisted")

    # Push lightweight summary
    context["ti"].xcom_push(key="crawl_counts", value={
        "groups":   len(data.get("groups",   [])),
        "topics":   len(data.get("topics",   [])),
        "problems": len(data.get("problems", [])),
        "answers":  len(data.get("answers",  [])),
    })


def _clean_and_save(**context):
    run_id  = context["ti"].xcom_pull(key="run_id")
    raw_key = context["ti"].xcom_pull(key="raw_key")

    from pipeline.storage.minio_client import get_datalake
    from pipeline.transforms.data_cleaner import clean

    lake = get_datalake()
    if raw_key and lake._client:
        raw_data = lake.load_json(raw_key)
    else:
        # Re-crawl if MinIO unavailable (last resort)
        from pipeline.crawlers.vnpt_scraper import VNPTScraper
        raw_data = VNPTScraper().run()

    clean_data = clean(raw_data)
    context["ti"].xcom_push(key="clean_stats", value=clean_data.get("cleaning_stats", {}))

    if lake._client:
        clean_key = lake.save_clean_json(run_id, clean_data)
        context["ti"].xcom_push(key="clean_key", value=clean_key)
        logger.info("Clean saved: %s", clean_key)
    else:
        context["ti"].xcom_push(key="clean_key", value="")


def _load_neo4j(**context):
    run_id    = context["ti"].xcom_pull(key="run_id")
    clean_key = context["ti"].xcom_pull(key="clean_key")
    clear_db  = context["params"].get("clear_neo4j", False)

    from pipeline.storage.minio_client import get_datalake
    from pipeline.loaders.neo4j_loader import Neo4jLoader

    lake = get_datalake()
    if clean_key and lake._client:
        clean_data = lake.load_json(clean_key)
    else:
        raise ValueError("No clean data available for Neo4j loading")

    loader = Neo4jLoader()
    try:
        counts = loader.load_from_dict(clean_data, clear=clear_db)
        context["ti"].xcom_push(key="neo4j_counts", value=counts)
        logger.info("Neo4j loaded: %s", counts)
    finally:
        loader.close()


def _export_csv(**context):
    """Export Neo4j graph to shared_data/ CSV files (integration bridge → Chatbot)."""
    import os
    from pipeline.loaders.neo4j_loader import Neo4jLoader

    out_dir = os.getenv("SHARED_DATA_DIR", "/shared_data")
    loader = Neo4jLoader()
    try:
        counts = loader.export_to_csv_dir(out_dir)
        context["ti"].xcom_push(key="csv_export_counts", value=counts)
        logger.info("CSV export to %s: %s", out_dir, counts)
    finally:
        loader.close()


def _backup(**context):
    run_id = context["ti"].xcom_pull(key="run_id")

    from pipeline.backup.backup_manager import BackupManager
    mgr      = BackupManager()
    manifest = mgr.run_backup(run_id)
    context["ti"].xcom_push(key="backup_manifest", value=manifest)
    logger.info("Backup: %s", manifest)


def _notify(**context):
    """Log final summary. Replace with Slack / email / Teams webhook as needed."""
    ti = context["ti"]
    summary = {
        "run_id":        ti.xcom_pull(key="run_id"),
        "crawl_counts":  ti.xcom_pull(key="crawl_counts"),
        "clean_stats":   ti.xcom_pull(key="clean_stats"),
        "neo4j_counts":  ti.xcom_pull(key="neo4j_counts"),
        "backup":        ti.xcom_pull(key="backup_manifest"),
        "dag_run":       str(context.get("dag_run")),
    }
    logger.info("Pipeline complete:\n%s", json.dumps(summary, ensure_ascii=False, indent=2))

    # --- Slack webhook (optional) -----------------------------------------
    slack_url = context["params"].get("slack_webhook_url", "")
    if slack_url:
        import requests  # type: ignore
        msg = (
            f":white_check_mark: *VNPT Money Pipeline* complete\n"
            f"run_id: `{summary['run_id']}`\n"
            f"problems: {summary.get('crawl_counts', {}).get('problems', '?')}\n"
            f"neo4j: {summary.get('neo4j_counts', {})}"
        )
        try:
            requests.post(slack_url, json={"text": msg}, timeout=5)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="vnpt_money_data_pipeline",
        default_args=DEFAULT_ARGS,
        description="Weekly crawl of vnptpay.vn/web/trogiup → MinIO → Neo4j",
        schedule_interval="0 2 * * 1",   # Every Monday 02:00 UTC
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["vnpt", "data-pipeline", "neo4j", "minio"],
        params={
            "use_playwright": False,
            "clear_neo4j":    False,
            "slack_webhook_url": "",
        },
    ) as dag:

        t_run_id = PythonOperator(
            task_id="generate_run_id",
            python_callable=_generate_run_id,
        )

        t_crawl = PythonOperator(
            task_id="crawl",
            python_callable=_crawl,
        )

        t_clean = PythonOperator(
            task_id="clean_and_save",
            python_callable=_clean_and_save,
        )

        t_load = PythonOperator(
            task_id="load_neo4j",
            python_callable=_load_neo4j,
        )

        t_export_csv = PythonOperator(
            task_id="export_csv_to_shared",
            python_callable=_export_csv,
        )

        t_backup = PythonOperator(
            task_id="backup",
            python_callable=_backup,
        )

        t_notify = PythonOperator(
            task_id="notify",
            python_callable=_notify,
            trigger_rule="all_done",   # Run even if upstream fails to still notify
        )

        t_run_id >> t_crawl >> t_clean >> t_load >> t_export_csv >> t_backup >> t_notify
