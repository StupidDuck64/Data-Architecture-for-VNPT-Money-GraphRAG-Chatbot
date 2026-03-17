"""
Main Pipeline Orchestrator
===========================
Ties all stages together:
  1. Crawl (VNPTScraper)
  2. Save raw to MinIO
  3. Clean  (data_cleaner)
  4. Save cleaned to MinIO
  5. Load into Neo4j (Neo4jLoader)
  6. Backup (BackupManager)

Can be called:
  - directly as a Python script
  - from the Airflow DAG (one task per stage)
  - programmatically by importing run_pipeline()
"""

import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from dotenv import load_dotenv  # type: ignore

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("pipeline.run")


# ---------------------------------------------------------------------------
# Stage functions (also used as individual Airflow tasks)
# ---------------------------------------------------------------------------

def stage_crawl(run_id: str, use_playwright: bool = False) -> Dict[str, Any]:
    """Fetch vnptpay.vn/web/trogiup and return raw structured data + raw HTML."""
    from pipeline.crawlers.vnpt_scraper import VNPTScraper

    scraper = VNPTScraper(use_playwright=use_playwright)
    data = scraper.run()
    data["_raw_html"] = scraper.get_raw_html() or ""
    data["_run_id"]   = run_id
    return data


def stage_save_raw(run_id: str, raw_data: Dict[str, Any]) -> str:
    """Upload raw HTML + raw JSON to MinIO, return the JSON key."""
    from pipeline.storage.minio_client import get_datalake

    lake = get_datalake()
    if lake._client is None:
        logger.warning("MinIO unavailable – skipping raw save")
        return ""

    html = raw_data.pop("_raw_html", "")
    if html:
        lake.save_raw_html(run_id, html)

    return lake.save_raw_json(run_id, raw_data)


def stage_clean(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Apply data cleaning / normalisation."""
    from pipeline.transforms.data_cleaner import clean

    return clean(raw_data)


def stage_save_clean(run_id: str, clean_data: Dict[str, Any]) -> str:
    """Upload cleaned JSON to MinIO, return the key."""
    from pipeline.storage.minio_client import get_datalake

    lake = get_datalake()
    if lake._client is None:
        logger.warning("MinIO unavailable – skipping clean save")
        return ""

    return lake.save_clean_json(run_id, clean_data)


def stage_load_neo4j(clean_data: Dict[str, Any], clear: bool = False) -> Dict[str, int]:
    """Upsert nodes / relationships into Neo4j."""
    from pipeline.loaders.neo4j_loader import Neo4jLoader

    loader = Neo4jLoader()
    try:
        counts = loader.load_from_dict(clean_data, clear=clear)
    finally:
        loader.close()
    return counts


def stage_export_csv(export_dir: str = None) -> Dict[str, int]:
    """
    Export current Neo4j graph to CSV files in the external_data layout.

    The output directory (default: SHARED_DATA_DIR env var or ./shared_data)
    is the bridge that the Chatbot's Neo4j import volume reads from.
    """
    from pipeline.loaders.neo4j_loader import Neo4jLoader

    out_dir = export_dir or os.getenv("SHARED_DATA_DIR", "./shared_data")
    loader = Neo4jLoader()
    try:
        counts = loader.export_to_csv_dir(out_dir)
    finally:
        loader.close()
    return counts


def stage_backup(run_id: str) -> Dict[str, Any]:
    """Export Neo4j graph to MinIO backup."""
    from pipeline.backup.backup_manager import BackupManager

    mgr = BackupManager()
    return mgr.run_backup(run_id)


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    run_id:         Optional[str] = None,
    use_playwright: bool          = False,
    clear_neo4j:    bool          = False,
    skip_backup:    bool          = False,
    export_dir:     Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute the complete pipeline end-to-end.

    Returns a summary dict.
    """
    run_id = run_id or f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    logger.info("=" * 60)
    logger.info("Pipeline START  run_id=%s", run_id)
    logger.info("=" * 60)

    summary: Dict[str, Any] = {"run_id": run_id, "started_at": datetime.utcnow().isoformat()}

    try:
        # 1. Crawl
        logger.info("[1/6] Crawling ...")
        raw = stage_crawl(run_id, use_playwright=use_playwright)
        summary["crawl"] = {
            "groups":   len(raw.get("groups",   [])),
            "topics":   len(raw.get("topics",   [])),
            "problems": len(raw.get("problems", [])),
            "answers":  len(raw.get("answers",  [])),
        }

        # 2. Save raw
        logger.info("[2/6] Saving raw data to MinIO ...")
        raw_key = stage_save_raw(run_id, raw)
        summary["raw_key"] = raw_key

        # 3. Clean
        logger.info("[3/6] Cleaning data ...")
        clean_data = stage_clean(raw)
        summary["clean"] = clean_data.get("cleaning_stats", {})

        # 4. Save clean
        logger.info("[4/6] Saving clean data to MinIO ...")
        clean_key = stage_save_clean(run_id, clean_data)
        summary["clean_key"] = clean_key

        # 5. Load Neo4j
        logger.info("[5/7] Loading into Neo4j ...")
        counts = stage_load_neo4j(clean_data, clear=clear_neo4j)
        summary["neo4j_counts"] = counts

        # 6. Export CSVs to shared_data/ (Integration bridge → Chatbot)
        logger.info("[6/7] Exporting CSVs to shared_data/ ...")
        csv_counts = stage_export_csv(export_dir)
        summary["csv_export"] = {"dir": export_dir or os.getenv("SHARED_DATA_DIR", "./shared_data"), "counts": csv_counts}

        # 7. Backup
        if not skip_backup:
            logger.info("[7/7] Backing up Neo4j graph ...")
            backup_manifest = stage_backup(run_id)
            summary["backup"] = backup_manifest
        else:
            logger.info("[7/7] Backup skipped")

        summary["status"] = "success"

    except Exception as exc:
        logger.exception("Pipeline FAILED: %s", exc)
        summary["status"] = "failed"
        summary["error"]  = str(exc)
        raise

    finally:
        summary["finished_at"] = datetime.utcnow().isoformat()
        logger.info("Pipeline END  status=%s  run_id=%s", summary.get("status"), run_id)

    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse, json as _json

    parser = argparse.ArgumentParser(description="VNPT Money Data Pipeline")
    parser.add_argument("--playwright",   action="store_true", help="Use Playwright for JS-rendered pages")
    parser.add_argument("--clear",        action="store_true", help="Clear Neo4j before loading")
    parser.add_argument("--skip-backup",  action="store_true", help="Skip Neo4j backup step")
    parser.add_argument("--run-id",       default=None,        help="Custom run identifier")
    parser.add_argument("--export-dir",   default=None,        help="Output dir for CSV export (default: SHARED_DATA_DIR env or ./shared_data)")
    args = parser.parse_args()

    result = run_pipeline(
        run_id=args.run_id,
        use_playwright=args.playwright,
        clear_neo4j=args.clear,
        skip_backup=args.skip_backup,
        export_dir=args.export_dir,
    )
    print(_json.dumps(result, ensure_ascii=False, indent=2))
