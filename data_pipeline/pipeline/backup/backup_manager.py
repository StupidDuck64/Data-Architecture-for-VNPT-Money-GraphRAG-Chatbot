"""
Backup Manager
==============
Handles two types of backup:
  1. Neo4j graph export → JSON → MinIO backup/ prefix
  2. MinIO data versioning snapshot manifest

Schedule: triggered by Airflow after every successful pipeline run.
Can also be run standalone.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class BackupManager:
    """
    Coordinates Neo4j → MinIO backups.

    Dependencies are injected so the class stays testable without live services.
    """

    def __init__(
        self,
        neo4j_uri:      Optional[str] = None,
        neo4j_user:     Optional[str] = None,
        neo4j_password: Optional[str] = None,
        neo4j_database: Optional[str] = None,
        minio_client=None,          # DataLakeClient instance
    ):
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()

        self.neo4j_uri      = neo4j_uri      or os.getenv("NEO4J_URI",      "bolt://localhost:7687")
        self.neo4j_user     = neo4j_user     or os.getenv("NEO4J_USER",     "neo4j")
        self.neo4j_password = neo4j_password or os.getenv("NEO4J_PASSWORD", "password")
        self.neo4j_database = neo4j_database or os.getenv("NEO4J_DATABASE", "neo4j")

        self._minio = minio_client

    # ------------------------------------------------------------------ #
    # Public
    # ------------------------------------------------------------------ #

    def run_backup(self, run_id: str) -> Dict[str, Any]:
        """
        Export all Neo4j nodes + relationships to JSON and upload to MinIO.

        Returns a manifest dict describing the backup.
        """
        logger.info("[backup] Starting backup for run_id=%s", run_id)

        export = self._export_neo4j()
        manifest = {
            "run_id":       run_id,
            "timestamp":    datetime.utcnow().isoformat(),
            "node_counts":  {k: len(v) for k, v in export.get("nodes", {}).items()},
            "rel_count":    len(export.get("relationships", [])),
            "storage_key":  None,
        }

        minio = self._get_minio()
        if minio:
            key = minio.save_backup(run_id, {"export": export, "manifest": manifest})
            manifest["storage_key"] = key
            logger.info("[backup] Uploaded to MinIO: %s", key)
        else:
            # Fallback: save locally
            local_path = f"backup_{run_id}.json"
            with open(local_path, "w", encoding="utf-8") as f:
                json.dump({"export": export, "manifest": manifest}, f, ensure_ascii=False, indent=2)
            manifest["storage_key"] = local_path
            logger.warning("[backup] MinIO unavailable – saved locally: %s", local_path)

        logger.info("[backup] Backup complete: %s", manifest)
        return manifest

    def restore_backup(self, run_id: str, clear_existing: bool = True) -> bool:
        """
        Download a backup from MinIO and restore it to Neo4j.
        """
        minio = self._get_minio()
        if not minio:
            logger.error("[restore] MinIO not available")
            return False

        key = minio.backup_key(run_id)
        try:
            payload = minio.load_json(key)
        except Exception as exc:
            logger.error("[restore] Cannot load backup %s: %s", key, exc)
            return False

        export = payload.get("export", {})
        logger.info("[restore] Restoring %d node types, %d rels",
                    len(export.get("nodes", {})),
                    len(export.get("relationships", [])))

        from pipeline.loaders.neo4j_loader import Neo4jLoader  # local import

        loader = Neo4jLoader(
            uri=self.neo4j_uri,
            user=self.neo4j_user,
            password=self.neo4j_password,
            database=self.neo4j_database,
        )
        try:
            # Rebuild data dict for the loader
            data = {
                "groups":           export["nodes"].get("Group",   []),
                "topics":           export["nodes"].get("Topic",   []),
                "problems":         export["nodes"].get("Problem", []),
                "answers":          export["nodes"].get("Answer",  []),
                "rels_has_topic":   [r for r in export["relationships"] if r.get("type") == "HAS_TOPIC"],
                "rels_has_problem": [r for r in export["relationships"] if r.get("type") == "HAS_PROBLEM"],
                "rels_has_answer":  [r for r in export["relationships"] if r.get("type") == "HAS_ANSWER"],
            }
            loader.load_from_dict(data, clear=clear_existing)
            return True
        finally:
            loader.close()

    def list_backups(self) -> List[str]:
        """Return list of backup keys in MinIO."""
        minio = self._get_minio()
        if not minio:
            return []
        return [k for k in minio.list_runs("backup")]

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #

    def _export_neo4j(self) -> Dict[str, Any]:
        """
        Export all nodes grouped by label and all relationships from Neo4j.
        """
        try:
            from neo4j import GraphDatabase  # type: ignore
            driver = GraphDatabase.driver(
                self.neo4j_uri,
                auth=(self.neo4j_user, self.neo4j_password),
            )
            nodes: Dict[str, List] = {}
            rels:  List[Dict]       = []

            with driver.session(database=self.neo4j_database) as session:
                # Export each label
                for label in ["Group", "Topic", "Problem", "Answer"]:
                    result = session.run(f"MATCH (n:{label}) RETURN properties(n) AS props")
                    nodes[label] = [r["props"] for r in result]

                # Export relationships (without full node props to keep size manageable)
                cypher = """
                MATCH (a)-[r]->(b)
                RETURN a.id AS from_id, type(r) AS type, b.id AS to_id
                """
                result = session.run(cypher)
                rels = [{"from_id": r["from_id"], "type": r["type"], "to_id": r["to_id"]} for r in result]

            driver.close()
            logger.info("[backup] Exported: %s", {k: len(v) for k, v in nodes.items()})
            return {"nodes": nodes, "relationships": rels}

        except Exception as exc:
            logger.error("[backup] Export failed: %s", exc)
            return {"nodes": {}, "relationships": [], "error": str(exc)}

    def _get_minio(self):
        if self._minio:
            return self._minio
        try:
            from pipeline.storage.minio_client import get_datalake
            return get_datalake()
        except Exception as exc:
            logger.warning("[backup] Cannot get MinIO client: %s", exc)
            return None
