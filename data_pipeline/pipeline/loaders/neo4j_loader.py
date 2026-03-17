"""
Neo4j Graph Loader
==================
Loads cleaned pipeline data into Neo4j using the same schema as
src/ingest_data_v3.py, keeping full backward-compatibility.

Node labels  : Group, Topic, Problem, Answer
              (+ ProblemSupplement, AnswerSupplement if present)
Relationships: (:Group)-[:HAS_TOPIC]->(:Topic)
               (:Topic)-[:HAS_PROBLEM]->(:Problem)
               (:Problem)-[:HAS_ANSWER]->(:Answer)

Key design decisions:
  - MERGE on id – safe to re-run (idempotent)
  - Batch writes for performance
  - Full-text & vector indexes created if they don't exist
  - Optional embedding generation via OpenAI / local model
"""

import csv
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from neo4j import GraphDatabase                 # type: ignore
from dotenv import load_dotenv                  # type: ignore

load_dotenv()

logger = logging.getLogger(__name__)

BATCH_SIZE = 200


class Neo4jLoader:
    """
    Loads structured data dicts into Neo4j.
    Accepts either:
      - A dict produced by the pipeline (groups/topics/problems/answers/rels)
      - A directory of CSV files matching the external_data layout
    """

    def __init__(
        self,
        uri:      Optional[str] = None,
        user:     Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
    ):
        self.uri      = uri      or os.getenv("NEO4J_URI",      "bolt://localhost:7687")
        self.user     = user     or os.getenv("NEO4J_USER",     "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "password")
        self.database = database or os.getenv("NEO4J_DATABASE", "neo4j")
        self.driver   = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        self.driver.close()

    # ------------------------------------------------------------------ #
    # Schema setup
    # ------------------------------------------------------------------ #

    def create_schema(self):
        """Create constraints and indexes (idempotent)."""
        constraints = [
            "CREATE CONSTRAINT group_id   IF NOT EXISTS FOR (n:Group)   REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT topic_id   IF NOT EXISTS FOR (n:Topic)   REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT problem_id IF NOT EXISTS FOR (n:Problem) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT answer_id  IF NOT EXISTS FOR (n:Answer)  REQUIRE n.id IS UNIQUE",
        ]
        indexes = [
            "CREATE INDEX problem_status IF NOT EXISTS FOR (p:Problem) ON (p.status)",
            "CREATE INDEX problem_intent IF NOT EXISTS FOR (p:Problem) ON (p.intent)",
            "CREATE FULLTEXT INDEX problem_text IF NOT EXISTS FOR (p:Problem) ON EACH [p.title, p.keywords]",
            "CREATE FULLTEXT INDEX answer_text  IF NOT EXISTS FOR (a:Answer)  ON EACH [a.summary, a.content]",
        ]
        with self.driver.session(database=self.database) as session:
            for stmt in constraints + indexes:
                try:
                    session.run(stmt)
                except Exception as e:
                    logger.debug("Schema stmt skipped (%s): %s", e, stmt[:60])
        logger.info("Schema ready")

    # ------------------------------------------------------------------ #
    # Main load entry-points
    # ------------------------------------------------------------------ #

    def load_from_dict(self, data: Dict[str, Any], clear: bool = False) -> Dict[str, int]:
        """
        Load all entities from a pipeline data dict.
        Returns counts of upserted nodes/rels.
        """
        if clear:
            self._clear()

        self.create_schema()

        counts = {}
        counts["groups"]   = self._load_groups(data.get("groups",   []))
        counts["topics"]   = self._load_topics(data.get("topics",   []))
        counts["problems"] = self._load_problems(data.get("problems", []))
        counts["answers"]  = self._load_answers(data.get("answers",  []))
        counts["rel_has_topic"]   = self._load_has_topic(data.get("rels_has_topic",   []))
        counts["rel_has_problem"] = self._load_has_problem(data.get("rels_has_problem", []))
        counts["rel_has_answer"]  = self._load_has_answer(data.get("rels_has_answer",  []))

        logger.info("Load complete: %s", counts)
        return counts

    def load_from_csv_dir(self, data_dir: str, clear: bool = False) -> Dict[str, int]:
        """Load from a directory of CSV files (external_data layout)."""
        p = Path(data_dir)
        data = {
            "groups":          _read_csv(p / "nodes_group.csv"),
            "topics":          _read_csv(p / "nodes_topic.csv"),
            "problems":        _read_csv(p / "nodes_problem.csv"),
            "answers":         _read_csv(p / "nodes_answer.csv"),
            "rels_has_topic":  _read_csv(p / "rels_has_topic.csv"),
            "rels_has_problem": _read_csv(p / "rels_has_problem.csv"),
            "rels_has_answer": _read_csv(p / "rels_has_answer.csv"),
        }
        # Optionally load supplement files
        for suffix in ["nodes_problem_supplement.csv", "nodes_answer_supplement.csv",
                       "rels_has_answer_supplement.csv", "rels_has_problem_supplement.csv"]:
            path = p / suffix
            if path.exists():
                key = suffix.replace(".csv", "")
                if "problem_supplement" in key:
                    data["problems"] += _read_csv(path)
                elif "answer_supplement" in key:
                    data["answers"] += _read_csv(path)
                elif "rels_has_answer" in key:
                    data["rels_has_answer"] += _read_csv(path)
                elif "rels_has_problem" in key:
                    data["rels_has_problem"] += _read_csv(path)

        return self.load_from_dict(data, clear=clear)

    # ------------------------------------------------------------------ #
    # Node loaders (batched MERGE)
    # ------------------------------------------------------------------ #

    def _load_groups(self, rows: List[Dict]) -> int:
        cypher = """
        UNWIND $batch AS g
        MERGE (n:Group {id: g.id})
        SET n.name        = g.name,
            n.description = coalesce(g.description, ''),
            n.order       = toInteger(coalesce(g.order, 0))
        """
        return self._batch_write(cypher, rows)

    def _load_topics(self, rows: List[Dict]) -> int:
        cypher = """
        UNWIND $batch AS t
        MERGE (n:Topic {id: t.id})
        SET n.name     = t.name,
            n.group_id = t.group_id,
            n.keywords = coalesce(t.keywords, ''),
            n.order    = toInteger(coalesce(t.order, 0))
        """
        return self._batch_write(cypher, rows)

    def _load_problems(self, rows: List[Dict]) -> int:
        cypher = """
        UNWIND $batch AS p
        MERGE (n:Problem {id: p.id})
        SET n.title            = p.title,
            n.description      = coalesce(p.description, ''),
            n.intent           = coalesce(p.intent, ''),
            n.keywords         = coalesce(p.keywords, ''),
            n.sample_questions = coalesce(p.sample_questions, ''),
            n.status           = coalesce(p.status, 'active')
        """
        return self._batch_write(cypher, rows)

    def _load_answers(self, rows: List[Dict]) -> int:
        cypher = """
        UNWIND $batch AS a
        MERGE (n:Answer {id: a.id})
        SET n.summary = coalesce(a.summary, ''),
            n.content = coalesce(a.content, ''),
            n.steps   = coalesce(a.steps, ''),
            n.notes   = coalesce(a.notes, ''),
            n.status  = coalesce(a.status, 'active')
        """
        return self._batch_write(cypher, rows)

    # ------------------------------------------------------------------ #
    # Relationship loaders
    # ------------------------------------------------------------------ #

    def _load_has_topic(self, rows: List[Dict]) -> int:
        cypher = """
        UNWIND $batch AS r
        MATCH (g:Group {id: r.group_id})
        MATCH (t:Topic {id: r.topic_id})
        MERGE (g)-[:HAS_TOPIC]->(t)
        """
        return self._batch_write(cypher, rows)

    def _load_has_problem(self, rows: List[Dict]) -> int:
        cypher = """
        UNWIND $batch AS r
        MATCH (t:Topic {id: r.topic_id})
        MATCH (p:Problem {id: r.problem_id})
        MERGE (t)-[:HAS_PROBLEM]->(p)
        """
        return self._batch_write(cypher, rows)

    def _load_has_answer(self, rows: List[Dict]) -> int:
        cypher = """
        UNWIND $batch AS r
        MATCH (p:Problem {id: r.problem_id})
        MATCH (a:Answer  {id: r.answer_id})
        MERGE (p)-[:HAS_ANSWER]->(a)
        """
        return self._batch_write(cypher, rows)

    # ------------------------------------------------------------------ #
    # Batch helper
    # ------------------------------------------------------------------ #

    def _batch_write(self, cypher: str, rows: List[Dict]) -> int:
        if not rows:
            return 0
        total = 0
        with self.driver.session(database=self.database) as session:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                session.run(cypher, {"batch": batch})
                total += len(batch)
        return total

    def _clear(self):
        logger.warning("Clearing entire database...")
        with self.driver.session(database=self.database) as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("Database cleared")

    # ------------------------------------------------------------------ #
    # Embedding (optional)
    # ------------------------------------------------------------------ #

    def generate_embeddings(self, model: str = "text-embedding-3-small"):
        """
        Generate OpenAI embeddings for all Problem nodes and store as
        n.embedding (float list). Requires OPENAI_API_KEY env var.
        """
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.warning("OPENAI_API_KEY not set – skipping embeddings")
            return

        try:
            from openai import OpenAI  # type: ignore
            client = OpenAI(api_key=api_key)
        except ImportError:
            logger.error("openai package not installed")
            return

        with self.driver.session(database=self.database) as session:
            problems = session.run(
                "MATCH (p:Problem) WHERE p.embedding IS NULL RETURN p.id AS id, p.title AS title"
            ).data()

        logger.info("Generating embeddings for %d problems", len(problems))
        chunk_size = 100
        for i in range(0, len(problems), chunk_size):
            batch = problems[i : i + chunk_size]
            texts = [f"{p['title']}" for p in batch]
            try:
                resp = client.embeddings.create(input=texts, model=model)
                with self.driver.session(database=self.database) as session:
                    for p, emb in zip(batch, resp.data):
                        session.run(
                            "MATCH (n:Problem {id: $id}) SET n.embedding = $emb",
                            {"id": p["id"], "emb": emb.embedding},
                        )
            except Exception as exc:
                logger.error("Embedding batch %d failed: %s", i, exc)
        logger.info("Embeddings complete")


    # ------------------------------------------------------------------ #
    # CSV Export  (Integration bridge → Chatbot)
    # ------------------------------------------------------------------ #

    def export_to_csv_dir(self, output_dir: str) -> Dict[str, int]:
        """
        Export the current Neo4j graph to CSV files in the external_data
        layout so the Chatbot's Neo4j can consume them via LOAD CSV or
        ingest_data_v3.py.

        Files written:
          nodes_group.csv, nodes_topic.csv, nodes_problem.csv,
          nodes_answer.csv, nodes_problem_supplement.csv,
          nodes_answer_supplement.csv,
          rels_has_topic.csv, rels_has_problem.csv, rels_has_answer.csv

        Returns a dict {filename: row_count}.
        """
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        counts: Dict[str, int] = {}

        with self.driver.session(database=self.database) as session:
            # ── Nodes ────────────────────────────────────────────────────
            _node_exports = {
                "nodes_group.csv": (
                    "MATCH (n:Group) RETURN n.id AS id, n.name AS name, "
                    "coalesce(n.description,'') AS description, coalesce(n.order,0) AS order"
                ),
                "nodes_topic.csv": (
                    "MATCH (n:Topic) RETURN n.id AS id, n.name AS name, "
                    "coalesce(n.group_id,'') AS group_id, coalesce(n.keywords,'') AS keywords, "
                    "coalesce(n.order,0) AS order"
                ),
                "nodes_problem.csv": (
                    "MATCH (n:Problem) WHERE NOT n.id CONTAINS '_sup' "
                    "RETURN n.id AS id, n.title AS title, "
                    "coalesce(n.description,'') AS description, coalesce(n.intent,'') AS intent, "
                    "coalesce(n.keywords,'') AS keywords, "
                    "coalesce(n.sample_questions,'') AS sample_questions, "
                    "coalesce(n.status,'active') AS status"
                ),
                "nodes_problem_supplement.csv": (
                    "MATCH (n:Problem) WHERE n.id CONTAINS '_sup' "
                    "RETURN n.id AS id, n.title AS title, "
                    "coalesce(n.description,'') AS description, coalesce(n.intent,'') AS intent, "
                    "coalesce(n.keywords,'') AS keywords, "
                    "coalesce(n.sample_questions,'') AS sample_questions, "
                    "coalesce(n.status,'active') AS status"
                ),
                "nodes_answer.csv": (
                    "MATCH (n:Answer) WHERE NOT n.id CONTAINS '_sup' "
                    "RETURN n.id AS id, coalesce(n.summary,'') AS summary, "
                    "coalesce(n.content,'') AS content, coalesce(n.steps,'') AS steps, "
                    "coalesce(n.notes,'') AS notes, coalesce(n.status,'active') AS status"
                ),
                "nodes_answer_supplement.csv": (
                    "MATCH (n:Answer) WHERE n.id CONTAINS '_sup' "
                    "RETURN n.id AS id, coalesce(n.summary,'') AS summary, "
                    "coalesce(n.content,'') AS content, coalesce(n.steps,'') AS steps, "
                    "coalesce(n.notes,'') AS notes, coalesce(n.status,'active') AS status"
                ),
            }
            # ── Relationships ────────────────────────────────────────────
            _rel_exports = {
                "rels_has_topic.csv": (
                    "MATCH (g:Group)-[:HAS_TOPIC]->(t:Topic) "
                    "RETURN g.id AS group_id, t.id AS topic_id"
                ),
                "rels_has_problem.csv": (
                    "MATCH (t:Topic)-[:HAS_PROBLEM]->(p:Problem) "
                    "RETURN t.id AS topic_id, p.id AS problem_id"
                ),
                "rels_has_answer.csv": (
                    "MATCH (p:Problem)-[:HAS_ANSWER]->(a:Answer) "
                    "RETURN p.id AS problem_id, a.id AS answer_id"
                ),
                "rels_has_answer_supplement.csv": (
                    "MATCH (p:Problem)-[:HAS_ANSWER]->(a:Answer) WHERE a.id CONTAINS '_sup' "
                    "RETURN p.id AS problem_id, a.id AS answer_id"
                ),
            }

            all_exports = {**_node_exports, **_rel_exports}
            for filename, cypher in all_exports.items():
                rows = session.run(cypher).data()
                _write_csv(out / filename, rows)
                counts[filename] = len(rows)
                logger.info("Exported %-40s  %d rows", filename, len(rows))

        logger.info("CSV export complete → %s  (%s)", output_dir, counts)
        return counts


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: List[Dict]) -> None:
    """Write a list of dicts to CSV (UTF-8 with BOM for Excel compatibility)."""
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))
