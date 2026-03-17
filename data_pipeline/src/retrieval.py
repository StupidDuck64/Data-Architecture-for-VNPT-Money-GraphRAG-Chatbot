import logging
import hashlib
from typing import List, Optional, Dict, Any
from functools import lru_cache

from neo4j import GraphDatabase

from schema import (
    StructuredQueryObject,
    CandidateProblem,
    RetrievedContext,
    SERVICE_GROUP_MAP,
    Config,
)

logger = logging.getLogger(__name__)


class EmbeddingCache:
    """Cache embedding để giảm API calls."""
    
    def __init__(self, max_size: int = 500):
        self.cache: Dict[str, List[float]] = {}
        self.max_size = max_size
        self.hits = 0
        self.misses = 0
    
    def _normalize_query(self, text: str) -> str:
        normalized = text.lower().strip()
        normalized = " ".join(normalized.split())
        return normalized
    
    def _hash_query(self, text: str) -> str:
        normalized = self._normalize_query(text)
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def get(self, text: str) -> Optional[List[float]]:
        key = self._hash_query(text)
        if key in self.cache:
            self.hits += 1
            logger.debug(f"Embedding cache HIT (hits={self.hits}, misses={self.misses})")
            return self.cache[key]
        self.misses += 1
        return None
    
    def set(self, text: str, embedding: List[float]) -> None:
        if len(self.cache) >= self.max_size:
            keys_to_remove = list(self.cache.keys())[:self.max_size // 10]
            for k in keys_to_remove:
                del self.cache[k]
        key = self._hash_query(text)
        self.cache[key] = embedding
    
    def stats(self) -> Dict[str, int]:
        return {
            "size": len(self.cache),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": self.hits / (self.hits + self.misses) if (self.hits + self.misses) > 0 else 0
        }


_embedding_cache = EmbeddingCache(max_size=500)


class GraphConstraintFilter:
    """Lọc không gian tìm kiếm dựa trên service/topic."""
    
    def __init__(self, neo4j_driver):
        self.driver = neo4j_driver
        self._group_cache: Dict[tuple, List[str]] = {}
    
    def get_constrained_problems(self, query: StructuredQueryObject) -> List[str]:
        service_value = query.service.value
        allowed_groups = SERVICE_GROUP_MAP.get(service_value, [])
        
        if not allowed_groups:
            allowed_groups = list(SERVICE_GROUP_MAP.get("khac", []))
        
        cache_key = tuple(sorted(allowed_groups))
        if cache_key in self._group_cache:
            problem_ids = self._group_cache[cache_key]
            logger.info(f"Constrained to {len(problem_ids)} Problems (cached)")
            return problem_ids
        
        cypher = """
        MATCH (g:Group)-[:HAS_TOPIC]->(t:Topic)-[:HAS_PROBLEM]->(p:Problem)
        WHERE g.id IN $allowed_groups AND p.status = 'active'
        RETURN DISTINCT p.id AS problem_id
        """
        
        with self.driver.session() as session:
            result = session.run(cypher, {"allowed_groups": allowed_groups})
            problem_ids = [record["problem_id"] for record in result]
        
        self._group_cache[cache_key] = problem_ids
        logger.info(f"Constrained to {len(problem_ids)} Problems from groups: {allowed_groups}")
        return problem_ids
    
    def get_all_active_problems(self) -> List[str]:
        cypher = "MATCH (p:Problem) WHERE p.status = 'active' RETURN p.id AS problem_id"
        with self.driver.session() as session:
            result = session.run(cypher)
            return [record["problem_id"] for record in result]


class ConstrainedVectorSearch:
    """Vector search trên tập Problem đã được lọc."""
    
    def __init__(self, neo4j_driver, embedding_client):
        self.driver = neo4j_driver
        self.embedding_client = embedding_client
        self.embedding_model = Config.EMBEDDING_MODEL
        self.top_k = Config.VECTOR_SEARCH_TOP_K
        self.cache = _embedding_cache
        self._embedding_available = True  # will be set False on first auth/quota error
    
    def embed(self, text: str) -> Optional[List[float]]:
        if not self._embedding_available or self.embedding_client is None:
            return None
        cached = self.cache.get(text)
        if cached is not None:
            return cached
        try:
            response = self.embedding_client.embeddings.create(model=self.embedding_model, input=text)
            embedding = response.data[0].embedding
            self.cache.set(text, embedding)
            return embedding
        except Exception as e:
            logger.warning(f"Embedding không khả dụng ({type(e).__name__}): {e}. Chuyển sang fulltext search.")
            self._embedding_available = False
            return None
    
    def search(self, query: str, constrained_ids: List[str], top_k: Optional[int] = None) -> List[CandidateProblem]:
        if not constrained_ids:
            logger.warning("Không có constrained IDs")
            return []
        
        top_k = top_k or self.top_k
        query_embedding = self.embed(query)
        
        if query_embedding is None:
            return []  # caller should use fulltext fallback
        
        cypher = """
        CALL db.index.vector.queryNodes('problem_embedding_index', $top_k * 2, $embedding)
        YIELD node, score
        WHERE node.id IN $constrained_ids
        RETURN node.id AS problem_id, node.title AS title, node.description AS description,
               node.intent AS intent, node.keywords AS keywords, score AS similarity_score
        ORDER BY score DESC
        LIMIT $top_k
        """
        
        with self.driver.session() as session:
            result = session.run(cypher, {"embedding": query_embedding, "constrained_ids": constrained_ids, "top_k": top_k})
            candidates = []
            for record in result:
                keywords = record["keywords"]
                if isinstance(keywords, str):
                    keywords = keywords.split(",") if keywords else []
                candidates.append(CandidateProblem(
                    problem_id=record["problem_id"],
                    title=record["title"],
                    description=record["description"],
                    intent=record["intent"],
                    keywords=keywords,
                    similarity_score=record["similarity_score"]
                ))
            return candidates
    
    def search_with_fallback(self, query: str, constrained_ids: List[str], all_problem_ids: List[str], top_k: Optional[int] = None) -> List[CandidateProblem]:
        candidates = self.search(query, constrained_ids, top_k)
        if len(candidates) < 3 and len(all_problem_ids) > len(constrained_ids):
            logger.info("Constrained search ít kết quả, mở rộng phạm vi")
            candidates = self.search(query, all_problem_ids, top_k)
        return candidates


class GraphTraversal:
    """Duyệt graph để lấy context đầy đủ."""
    
    def __init__(self, neo4j_driver):
        self.driver = neo4j_driver
    
    def fetch_context(self, problem_ids: List[str]) -> List[RetrievedContext]:
        if not problem_ids:
            return []
        
        cypher = """
        MATCH (p:Problem)-[:HAS_ANSWER]->(a:Answer)
        WHERE p.id IN $problem_ids
        MATCH (g:Group)-[:HAS_TOPIC]->(t:Topic)-[:HAS_PROBLEM]->(p)
        RETURN p.id AS problem_id, p.title AS problem_title, a.id AS answer_id,
               a.content AS answer_content, a.steps AS answer_steps, a.notes AS answer_notes,
               t.id AS topic_id, t.name AS topic_name, g.id AS group_id, g.name AS group_name
        """
        
        with self.driver.session() as session:
            result = session.run(cypher, {"problem_ids": problem_ids})
            contexts = []
            for record in result:
                steps = record["answer_steps"]
                if isinstance(steps, str):
                    steps = steps.split("\n") if steps else None
                contexts.append(RetrievedContext(
                    problem_id=record["problem_id"],
                    problem_title=record["problem_title"],
                    answer_id=record["answer_id"],
                    answer_content=record["answer_content"],
                    answer_steps=steps,
                    answer_notes=record["answer_notes"],
                    topic_id=record["topic_id"],
                    topic_name=record["topic_name"],
                    group_id=record["group_id"],
                    group_name=record["group_name"]
                ))
            return contexts
    
    def get_context_for_problem(self, problem_id: str) -> Optional[RetrievedContext]:
        contexts = self.fetch_context([problem_id])
        return contexts[0] if contexts else None


class RetrievalPipeline:
    """Pipeline retrieval hoàn chỉnh."""
    
    def __init__(self, neo4j_driver, embedding_client):
        self.constraint_filter = GraphConstraintFilter(neo4j_driver)
        self.vector_search = ConstrainedVectorSearch(neo4j_driver, embedding_client)
        self.graph_traversal = GraphTraversal(neo4j_driver)
        self.driver = neo4j_driver
    
    def fulltext_search(self, query: str, constrained_ids: Optional[List[str]], top_k: int = 5) -> List[CandidateProblem]:
        """Fallback: Neo4j fulltext search khi không có embedding."""
        # Sanitize query for Lucene fulltext
        safe_query = ' '.join(
            word for word in query.replace('"', '').replace("'", '').split()
        )
        if not safe_query:
            safe_query = query
        cypher = """
        CALL db.index.fulltext.queryNodes('problem_text', $query)
        YIELD node, score
        WHERE node.status = 'active'
        AND ($constrained IS NULL OR node.id IN $constrained)
        RETURN node.id AS problem_id, node.title AS title,
               node.description AS description,
               node.intent AS intent, node.keywords AS keywords,
               score AS similarity_score
        LIMIT $top_k
        """
        try:
            with self.driver.session() as session:
                result = session.run(cypher, {
                    "query": safe_query,
                    "constrained": constrained_ids,
                    "top_k": top_k
                })
                candidates = []
                for record in result:
                    keywords = record["keywords"]
                    if isinstance(keywords, str):
                        keywords = keywords.split(",") if keywords else []
                    candidates.append(CandidateProblem(
                        problem_id=record["problem_id"],
                        title=record["title"],
                        description=record["description"],
                        intent=record["intent"],
                        keywords=keywords,
                        similarity_score=float(record["similarity_score"])
                    ))
                return candidates
        except Exception as e:
            logger.warning(f"Fulltext search lỗi: {e}")
            return []

    def retrieve(self, query: StructuredQueryObject, top_k: Optional[int] = None) -> tuple[List[CandidateProblem], List[RetrievedContext]]:
        constrained_ids = self.constraint_filter.get_constrained_problems(query)
        logger.info(f"Constrained to {len(constrained_ids)} Problems")
        
        candidates = self.vector_search.search(query.condensed_query, constrained_ids, top_k)
        if not candidates:
            logger.info("Vector search không có kết quả, dùng fulltext search")
            candidates = self.fulltext_search(query.condensed_query, constrained_ids or None, top_k or Config.VECTOR_SEARCH_TOP_K)
        logger.info(f"Found {len(candidates)} candidates")
        
        problem_ids = [c.problem_id for c in candidates]
        contexts = self.graph_traversal.fetch_context(problem_ids)
        logger.info(f"Retrieved {len(contexts)} contexts")
        
        return candidates, contexts
    
    def retrieve_with_fallback(self, query: StructuredQueryObject, top_k: Optional[int] = None) -> tuple[List[CandidateProblem], List[RetrievedContext]]:
        constrained_ids = self.constraint_filter.get_constrained_problems(query)
        all_ids = self.constraint_filter.get_all_active_problems()
        candidates = self.vector_search.search_with_fallback(query.condensed_query, constrained_ids, all_ids, top_k)
        if not candidates:
            logger.info("Vector search thất bại, dùng fulltext search")
            candidates = self.fulltext_search(
                query.condensed_query,
                constrained_ids if constrained_ids else None,
                top_k or Config.VECTOR_SEARCH_TOP_K
            )
            if not candidates and constrained_ids:
                # Mở rộng sang toàn bộ
                candidates = self.fulltext_search(query.condensed_query, None, top_k or Config.VECTOR_SEARCH_TOP_K)
        problem_ids = [c.problem_id for c in candidates]
        contexts = self.graph_traversal.fetch_context(problem_ids)
        return candidates, contexts
