"""
MinIO Data Lake Client
======================
Manages two tiers:
  raw/   – original HTML + raw JSON from the scraper
  clean/ – processed, normalised JSON ready for Neo4j

Bucket layout:
  vnpt-datalake/
    raw/YYYY/MM/DD/HH/<run_id>/
      page.html
      raw_data.json
    clean/YYYY/MM/DD/HH/<run_id>/
      cleaned_data.json
    backup/<run_id>/
      neo4j_export.json
"""

import io
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from minio import Minio                          # type: ignore
    from minio.error import S3Error                  # type: ignore
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    logger.warning("minio package not installed – storage layer disabled")


class DataLakeClient:
    """
    Thin wrapper around the MinIO SDK.

    Environment variables (or pass to __init__):
      MINIO_ENDPOINT    e.g. localhost:9000
      MINIO_ACCESS_KEY
      MINIO_SECRET_KEY
      MINIO_BUCKET      default: vnpt-datalake
      MINIO_SECURE      1 / 0 (default 0)
    """

    def __init__(
        self,
        endpoint:   Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket:     Optional[str] = None,
        secure:     bool          = False,
    ):
        self.endpoint   = endpoint   or os.getenv("MINIO_ENDPOINT",    "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket     = bucket     or os.getenv("MINIO_BUCKET",     "vnpt-datalake")
        self.secure     = secure or bool(int(os.getenv("MINIO_SECURE", "0")))
        self._client: Optional[Any] = None

    # ------------------------------------------------------------------ #
    # Connection
    # ------------------------------------------------------------------ #

    def connect(self) -> bool:
        if not MINIO_AVAILABLE:
            logger.error("minio package not available")
            return False
        try:
            self._client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
            )
            self._ensure_bucket()
            logger.info("MinIO connected – bucket: %s", self.bucket)
            return True
        except Exception as exc:
            logger.error("MinIO connection failed: %s", exc)
            return False

    def _ensure_bucket(self):
        if not self._client.bucket_exists(self.bucket):
            self._client.make_bucket(self.bucket)
            logger.info("Created bucket: %s", self.bucket)
            # Set lifecycle policy: raw data retained 90 days, clean 365 days
            self._set_lifecycle()

    def _set_lifecycle(self):
        """Set retention lifecycle rules on MinIO bucket."""
        try:
            from minio.lifecycleconfig import (                           # type: ignore
                LifecycleConfig, Rule, Expiration, Filter,
            )
            config = LifecycleConfig(
                rules=[
                    Rule(
                        rule_filter=Filter(prefix="raw/"),
                        rule_id="expire-raw-90d",
                        status="Enabled",
                        expiration=Expiration(days=90),
                    ),
                    Rule(
                        rule_filter=Filter(prefix="clean/"),
                        rule_id="expire-clean-365d",
                        status="Enabled",
                        expiration=Expiration(days=365),
                    ),
                ]
            )
            self._client.set_bucket_lifecycle(self.bucket, config)
            logger.info("Lifecycle policy applied")
        except Exception as exc:
            logger.warning("Could not set lifecycle policy: %s", exc)

    # ------------------------------------------------------------------ #
    # Key builders
    # ------------------------------------------------------------------ #

    @staticmethod
    def _time_prefix(run_id: str, tier: str) -> str:
        now = datetime.utcnow()
        return f"{tier}/{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/{run_id}"

    def raw_html_key(self, run_id: str) -> str:
        return f"{self._time_prefix(run_id, 'raw')}/page.html"

    def raw_json_key(self, run_id: str) -> str:
        return f"{self._time_prefix(run_id, 'raw')}/raw_data.json"

    def clean_json_key(self, run_id: str) -> str:
        return f"{self._time_prefix(run_id, 'clean')}/cleaned_data.json"

    def backup_key(self, run_id: str) -> str:
        return f"backup/{run_id}/neo4j_export.json"

    # ------------------------------------------------------------------ #
    # Upload helpers
    # ------------------------------------------------------------------ #

    def _put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream"):
        if self._client is None:
            raise RuntimeError("Not connected – call connect() first")
        stream = io.BytesIO(data)
        self._client.put_object(
            self.bucket, key, stream,
            length=len(data), content_type=content_type,
        )
        logger.info("Uploaded s3://%s/%s (%d bytes)", self.bucket, key, len(data))

    def save_raw_html(self, run_id: str, html: str):
        key = self.raw_html_key(run_id)
        self._put_bytes(key, html.encode("utf-8"), "text/html; charset=utf-8")
        return key

    def save_raw_json(self, run_id: str, data: Any) -> str:
        key = self.raw_json_key(run_id)
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self._put_bytes(key, payload, "application/json")
        return key

    def save_clean_json(self, run_id: str, data: Any) -> str:
        key = self.clean_json_key(run_id)
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self._put_bytes(key, payload, "application/json")
        return key

    def save_backup(self, run_id: str, data: Any) -> str:
        key = self.backup_key(run_id)
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self._put_bytes(key, payload, "application/json")
        return key

    # ------------------------------------------------------------------ #
    # Download helpers
    # ------------------------------------------------------------------ #

    def load_json(self, key: str) -> Any:
        if self._client is None:
            raise RuntimeError("Not connected")
        response = self._client.get_object(self.bucket, key)
        data = response.read()
        response.close()
        return json.loads(data.decode("utf-8"))

    def load_text(self, key: str) -> str:
        if self._client is None:
            raise RuntimeError("Not connected")
        response = self._client.get_object(self.bucket, key)
        data = response.read()
        response.close()
        return data.decode("utf-8")

    # ------------------------------------------------------------------ #
    # List helpers
    # ------------------------------------------------------------------ #

    def list_runs(self, tier: str = "raw") -> List[str]:
        """Return list of object keys under a given tier prefix."""
        if self._client is None:
            raise RuntimeError("Not connected")
        objects = self._client.list_objects(self.bucket, prefix=f"{tier}/", recursive=True)
        return [obj.object_name for obj in objects]

    def get_latest_clean_key(self) -> Optional[str]:
        """Find the most recently uploaded clean JSON key."""
        keys = self.list_runs("clean")
        if not keys:
            return None
        return sorted(keys)[-1]


# ------------------------------------------------------------------ #
# Factory
# ------------------------------------------------------------------ #

_lake_instance: Optional[DataLakeClient] = None


def get_datalake() -> DataLakeClient:
    global _lake_instance
    if _lake_instance is None:
        _lake_instance = DataLakeClient()
        _lake_instance.connect()
    return _lake_instance
