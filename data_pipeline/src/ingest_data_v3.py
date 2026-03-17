import os
import csv
import logging
from typing import List, Dict, Any
from pathlib import Path

from neo4j import GraphDatabase
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataIngestion:
    """Nạp dữ liệu CSV vào Neo4j."""
    
    def __init__(
        self,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        openai_api_key: str = None,
        data_dir: str = "external_data_v3"
    ):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.data_dir = Path(data_dir)
        self.supplement_dir = Path("db/import")  # Secondary directory for supplement files
        
        if openai_api_key:
            self.openai = OpenAI(api_key=openai_api_key)
        else:
            self.openai = None
            logger.warning("Không có OpenAI key - bỏ qua embeddings")
    
    def close(self):
        self.driver.close()
    
    def clear_database(self):
        logger.info("Xóa database...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("Database đã xóa")
    
  
    def create_constraints(self):
        logger.info("Tạo constraints và indexes...")
        constraints = [
            "CREATE CONSTRAINT group_id IF NOT EXISTS FOR (g:Group) REQUIRE g.id IS UNIQUE",
            "CREATE CONSTRAINT topic_id IF NOT EXISTS FOR (t:Topic) REQUIRE t.id IS UNIQUE",
            "CREATE CONSTRAINT problem_id IF NOT EXISTS FOR (p:Problem) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT answer_id IF NOT EXISTS FOR (a:Answer) REQUIRE a.id IS UNIQUE",
        ]
        indexes = [
            "CREATE INDEX problem_status IF NOT EXISTS FOR (p:Problem) ON (p.status)",
            "CREATE INDEX problem_intent IF NOT EXISTS FOR (p:Problem) ON (p.intent)",
            "CREATE FULLTEXT INDEX problem_text IF NOT EXISTS FOR (p:Problem) ON EACH [p.title, p.keywords]",
        ]
        with self.driver.session() as session:
            for c in constraints:
                try:
                    session.run(c)
                except Exception as e:
                    logger.warning(f"Constraint có thể đã tồn tại: {e}")
            for i in indexes:
                try:
                    session.run(i)
                except Exception as e:
                    logger.warning(f"Index có thể đã tồn tại: {e}")
        logger.info("Đã tạo constraints và indexes")
    
    def read_csv(self, filename: str) -> List[Dict[str, Any]]:
        # Try primary data directory first
        filepath = self.data_dir / filename
        if not filepath.exists():
            # Fallback to supplement directory
            filepath = self.supplement_dir / filename
        if not filepath.exists():
            logger.warning(f"Không tìm thấy file: {filename}")
            return []
        with open(filepath, "r", encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))
    
    def ingest_groups(self):
        logger.info("Nạp Groups...")
        groups = self.read_csv("nodes_group.csv")
        with self.driver.session() as session:
            for g in groups:
                session.run("""
                    MERGE (g:Group {id: $id})
                    SET g.name = $name, g.description = $description, g.order = toInteger($order)
                """, {"id": g["id"], "name": g["name"], "description": g.get("description", ""), "order": g.get("order", 0)})
        logger.info(f"Đã nạp {len(groups)} Groups")
    
    def ingest_topics(self):
        logger.info("Nạp Topics...")
        topics = self.read_csv("nodes_topic.csv")
        with self.driver.session() as session:
            for t in topics:
                session.run("""
                    MERGE (t:Topic {id: $id})
                    SET t.name = $name, t.group_id = $group_id, t.keywords = $keywords, t.order = toInteger($order)
                """, {"id": t["id"], "name": t["name"], "group_id": t.get("group_id", ""), "keywords": t.get("keywords", ""), "order": t.get("order", 0)})
        logger.info(f"Đã nạp {len(topics)} Topics")
    
    def ingest_problems(self):
        logger.info("Nạp Problems...")
        problems = self.read_csv("nodes_problem.csv")
        
        # Also load supplement problems if exists
        supplement_problems = self.read_csv("nodes_problem_supplement.csv")
        if supplement_problems:
            logger.info(f"Tìm thấy {len(supplement_problems)} supplement problems")
            problems.extend(supplement_problems)
        
        with self.driver.session() as session:
            for p in problems:
                session.run("""
                    MERGE (p:Problem {id: $id})
                    SET p.title = $title, p.description = $description, p.intent = $intent, 
                        p.keywords = $keywords, p.sample_questions = $sample_questions, p.status = $status
                """, {"id": p["id"], "title": p["title"], "description": p.get("description", ""), 
                      "intent": p.get("intent", ""), "keywords": p.get("keywords", ""), 
                      "sample_questions": p.get("sample_questions", ""), "status": p.get("status", "active")})
        logger.info(f"Đã nạp {len(problems)} Problems")
    
    def ingest_answers(self):
        logger.info("Nạp Answers...")
        answers = self.read_csv("nodes_answer.csv")
        
        # Also load supplement answers if exists
        supplement_answers = self.read_csv("nodes_answer_supplement.csv")
        if supplement_answers:
            logger.info(f"Tìm thấy {len(supplement_answers)} supplement answers")
            answers.extend(supplement_answers)
        
        with self.driver.session() as session:
            for a in answers:
                session.run("""
                    MERGE (a:Answer {id: $id})
                    SET a.summary = $summary, a.content = $content, a.steps = $steps, a.notes = $notes, a.status = $status
                """, {"id": a["id"], "summary": a.get("summary", a.get("title", "")), "content": a.get("content", ""), 
                      "steps": a.get("steps", ""), "notes": a.get("notes", ""), "status": a.get("status", "active")})
        logger.info(f"Đã nạp {len(answers)} Answers")
    
    def create_relationships(self):
        logger.info("Tạo relationships...")
        self._create_has_topic_rels()
        self._create_has_problem_rels()
        self._create_has_answer_rels()
        logger.info("Đã tạo relationships")
    
    def _create_has_topic_rels(self):
        rels = self.read_csv("rels_has_topic.csv")
        with self.driver.session() as session:
            for r in rels:
                session.run("MATCH (g:Group {id: $start_id}) MATCH (t:Topic {id: $end_id}) MERGE (g)-[:HAS_TOPIC]->(t)", 
                           {"start_id": r["start_id"], "end_id": r["end_id"]})
        logger.info(f"Đã tạo {len(rels)} HAS_TOPIC")
    
    def _create_has_problem_rels(self):
        rels = self.read_csv("rels_has_problem.csv")
        
        # Also load supplement relationships
        supplement_rels = self.read_csv("rels_has_problem_supplement.csv")
        if supplement_rels:
            logger.info(f"Tìm thấy {len(supplement_rels)} supplement problem relationships")
            rels.extend(supplement_rels)
        
        with self.driver.session() as session:
            for r in rels:
                session.run("MATCH (t:Topic {id: $start_id}) MATCH (p:Problem {id: $end_id}) MERGE (t)-[:HAS_PROBLEM]->(p)", 
                           {"start_id": r["start_id"], "end_id": r["end_id"]})
        logger.info(f"Đã tạo {len(rels)} HAS_PROBLEM")
    
    def _create_has_answer_rels(self):
        rels = self.read_csv("rels_has_answer.csv")
        
        # Also load supplement relationships
        supplement_rels = self.read_csv("rels_has_answer_supplement.csv")
        if supplement_rels:
            logger.info(f"Tìm thấy {len(supplement_rels)} supplement answer relationships")
            rels.extend(supplement_rels)
        
        with self.driver.session() as session:
            for r in rels:
                session.run("MATCH (p:Problem {id: $start_id}) MATCH (a:Answer {id: $end_id}) MERGE (p)-[:HAS_ANSWER]->(a)", 
                           {"start_id": r["start_id"], "end_id": r["end_id"]})
        logger.info(f"Đã tạo {len(rels)} HAS_ANSWER")
    
    def generate_embeddings(self, batch_size: int = 50):
        if not self.openai:
            logger.warning("Bỏ qua embeddings - không có OpenAI client")
            return
        logger.info("Tạo embeddings cho Problems...")
        with self.driver.session() as session:
            result = session.run("MATCH (p:Problem) WHERE p.embedding IS NULL RETURN p.id AS id, p.title AS title, p.description AS description")
            problems = list(result)
        logger.info(f"Tìm thấy {len(problems)} problems cần embed")
        for i in range(0, len(problems), batch_size):
            batch = problems[i:i + batch_size]
            texts, ids = [], []
            for p in batch:
                texts.append(f"{p['title']} {p['description'] or ''}".strip())
                ids.append(p["id"])
            try:
                response = self.openai.embeddings.create(model="text-embedding-3-small", input=texts)
                with self.driver.session() as session:
                    for j, emb in enumerate(response.data):
                        session.run("MATCH (p:Problem {id: $id}) SET p.embedding = $embedding", {"id": ids[j], "embedding": emb.embedding})
                logger.info(f"Đã embed batch {i//batch_size + 1}")
            except Exception as e:
                logger.error(f"Lỗi tạo embeddings: {e}")
        logger.info("Đã tạo embeddings")
    
    def create_vector_index(self):
        logger.info("Tạo vector index...")
        try:
            with self.driver.session() as session:
                session.run("""
                    CREATE VECTOR INDEX problem_embedding_index IF NOT EXISTS
                    FOR (p:Problem) ON p.embedding
                    OPTIONS {indexConfig: {`vector.dimensions`: 1536, `vector.similarity_function`: 'cosine'}}
                """)
            logger.info("Đã tạo vector index")
        except Exception as e:
            logger.warning(f"Vector index có thể đã tồn tại: {e}")
    
    def run_full_ingestion(self, clear: bool = True, generate_embeddings: bool = True):
        logger.info("Bắt đầu nạp dữ liệu...")
        if clear:
            self.clear_database()
        self.create_constraints()
        self.ingest_groups()
        self.ingest_topics()
        self.ingest_problems()
        self.ingest_answers()
        self.create_relationships()
        if generate_embeddings:
            self.generate_embeddings()
            self.create_vector_index()
        self._print_summary()
        logger.info("Hoàn thành nạp dữ liệu!")
    
    def ingest_supplement_only(self, generate_embeddings: bool = True):
        """Chỉ ingest supplement data mà không xóa database."""
        logger.info("Bắt đầu nạp supplement data...")
        
        # Ingest supplement problems
        supplement_problems = self.read_csv("nodes_problem_supplement.csv")
        if supplement_problems:
            logger.info(f"Nạp {len(supplement_problems)} supplement problems...")
            with self.driver.session() as session:
                for p in supplement_problems:
                    session.run("""
                        MERGE (p:Problem {id: $id})
                        SET p.title = $title, p.description = $description, p.intent = $intent, 
                            p.keywords = $keywords, p.sample_questions = $sample_questions, p.status = $status
                    """, {"id": p["id"], "title": p["title"], "description": p.get("description", ""), 
                          "intent": p.get("intent", ""), "keywords": p.get("keywords", ""), 
                          "sample_questions": p.get("sample_questions", ""), "status": p.get("status", "active")})
        
        # Ingest supplement answers
        supplement_answers = self.read_csv("nodes_answer_supplement.csv")
        if supplement_answers:
            logger.info(f"Nạp {len(supplement_answers)} supplement answers...")
            with self.driver.session() as session:
                for a in supplement_answers:
                    session.run("""
                        MERGE (a:Answer {id: $id})
                        SET a.summary = $summary, a.content = $content, a.steps = $steps, a.notes = $notes, a.status = $status
                    """, {"id": a["id"], "summary": a.get("summary", a.get("title", "")), "content": a.get("content", ""), 
                          "steps": a.get("steps", ""), "notes": a.get("notes", ""), "status": a.get("status", "active")})
        
        # Create supplement relationships
        supplement_problem_rels = self.read_csv("rels_has_problem_supplement.csv")
        if supplement_problem_rels:
            logger.info(f"Tạo {len(supplement_problem_rels)} supplement problem relationships...")
            with self.driver.session() as session:
                for r in supplement_problem_rels:
                    session.run("MATCH (t:Topic {id: $start_id}) MATCH (p:Problem {id: $end_id}) MERGE (t)-[:HAS_PROBLEM]->(p)", 
                               {"start_id": r["start_id"], "end_id": r["end_id"]})
        
        supplement_answer_rels = self.read_csv("rels_has_answer_supplement.csv")
        if supplement_answer_rels:
            logger.info(f"Tạo {len(supplement_answer_rels)} supplement answer relationships...")
            with self.driver.session() as session:
                for r in supplement_answer_rels:
                    session.run("MATCH (p:Problem {id: $start_id}) MATCH (a:Answer {id: $end_id}) MERGE (p)-[:HAS_ANSWER]->(a)", 
                               {"start_id": r["start_id"], "end_id": r["end_id"]})
        
        # Generate embeddings for new problems
        if generate_embeddings:
            self.generate_embeddings()
        
        self._print_summary()
        logger.info("Hoàn thành nạp supplement data!")
    
    def _print_summary(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (g:Group) WITH count(g) AS groups
                MATCH (t:Topic) WITH groups, count(t) AS topics
                MATCH (p:Problem) WITH groups, topics, count(p) AS problems
                MATCH (a:Answer) WITH groups, topics, problems, count(a) AS answers
                RETURN groups, topics, problems, answers
            """)
            s = result.single()
            logger.info(f"Thống kê: Groups={s['groups']}, Topics={s['topics']}, Problems={s['problems']}, Answers={s['answers']}")


def main():
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    data_dir = os.getenv("DATA_DIR", "external_data_v3")
    ingestion = DataIngestion(neo4j_uri=neo4j_uri, neo4j_user=neo4j_user, neo4j_password=neo4j_password, openai_api_key=openai_api_key, data_dir=data_dir)
    try:
        ingestion.run_full_ingestion(clear=True, generate_embeddings=bool(openai_api_key))
    finally:
        ingestion.close()


if __name__ == "__main__":
    main()
