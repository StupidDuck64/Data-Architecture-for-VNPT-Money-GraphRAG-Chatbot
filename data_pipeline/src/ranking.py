import re
import logging
from typing import List, Dict, Optional
from collections import defaultdict

from schema import (
    StructuredQueryObject,
    CandidateProblem,
    RetrievedContext,
    RankedResult,
    RankingOutput,
    ConfidenceMetrics,
    Config,
)

logger = logging.getLogger(__name__)


class KeywordMatcher:
    """BM25-style keyword matching cho ranking."""
    
    def __init__(self):
        self.stopwords = {
            "va", "hoac", "la", "cua", "cho", "voi", "trong", "tren", "duoi",
            "nay", "do", "kia", "de", "vi", "nen", "nhung", "ma", "thi",
            "co", "khong", "duoc", "bi", "da", "dang", "se", "roi",
            "toi", "ban", "minh", "no", "ho", "chung", "ta", "cac"
        }
    
    def tokenize(self, text: str) -> List[str]:
        text = text.lower()
        text = re.sub(r"[^\w\s]", " ", text)
        tokens = [t.strip() for t in text.split() if t.strip()]
        return [t for t in tokens if t not in self.stopwords]
    
    def compute_overlap_score(self, query_tokens: List[str], doc_tokens: List[str]) -> float:
        if not query_tokens:
            return 0.0
        query_set = set(query_tokens)
        doc_set = set(doc_tokens)
        overlap = len(query_set & doc_set)
        return overlap / len(query_set)
    
    def score_candidate(self, query: str, candidate: CandidateProblem) -> float:
        query_tokens = self.tokenize(query)
        doc_text = f"{candidate.title} {candidate.description or ''}"
        doc_text += " ".join(candidate.keywords or [])
        doc_tokens = self.tokenize(doc_text)
        return self.compute_overlap_score(query_tokens, doc_tokens)


class GraphDistanceScorer:
    """Tính điểm dựa trên khoảng cách graph."""
    
    def score(self, candidate: CandidateProblem, context: Optional[RetrievedContext], query: StructuredQueryObject) -> float:
        if not context:
            return 0.3
        if query.topic and context.topic_id == query.topic:
            return 1.0
        from schema import SERVICE_GROUP_MAP
        expected_groups = SERVICE_GROUP_MAP.get(query.service.value, [])
        if context.group_id in expected_groups:
            return 0.8
        return 0.5


class IntentAlignmentScorer:
    """Tính điểm dựa trên sự phù hợp intent."""
    
    INTENT_SIMILARITY = {
        ("that_bai", "loi_ket_noi"): 0.8,
        ("that_bai", "pending_lau"): 0.7,
        ("tru_tien_chua_nhan", "pending_lau"): 0.9,
        ("khong_nhan_otp", "that_bai"): 0.6,
        ("huong_dan", "chinh_sach"): 0.7,
    }
    
    def score(self, candidate: CandidateProblem, query: StructuredQueryObject) -> float:
        query_intent = query.problem_type.value
        candidate_intent = candidate.intent
        if not candidate_intent:
            return 0.5
        candidate_intent = candidate_intent.lower().replace(" ", "_")
        if query_intent == candidate_intent:
            return 1.0
        pair = tuple(sorted([query_intent, candidate_intent]))
        if pair in self.INTENT_SIMILARITY:
            return self.INTENT_SIMILARITY[pair]
        return 0.3


class MultiSignalRanker:
    """Multi-signal ranking sử dụng RRF."""
    
    def __init__(self):
        self.keyword_matcher = KeywordMatcher()
        self.graph_scorer = GraphDistanceScorer()
        self.intent_scorer = IntentAlignmentScorer()
        self.k = Config.RRF_K
        self.weights = Config.RANKING_WEIGHTS
    
    def rank(self, candidates: List[CandidateProblem], contexts: List[RetrievedContext], query: StructuredQueryObject) -> RankingOutput:
        if not candidates:
            return RankingOutput(results=[], confidence_score=0.0, score_gap=0.0, is_ambiguous=True)
        
        context_map = {c.problem_id: c for c in contexts}
        
        vector_scores = self._get_vector_scores(candidates)
        keyword_scores = self._get_keyword_scores(candidates, query.condensed_query)
        graph_scores = self._get_graph_scores(candidates, context_map, query)
        intent_scores = self._get_intent_scores(candidates, query)
        
        vector_ranks = self._scores_to_ranks(vector_scores)
        keyword_ranks = self._scores_to_ranks(keyword_scores)
        graph_ranks = self._scores_to_ranks(graph_scores)
        intent_ranks = self._scores_to_ranks(intent_scores)
        
        rrf_scores = {}
        for candidate in candidates:
            pid = candidate.problem_id
            rrf_scores[pid] = (
                self.weights["vector"] * (1 / (self.k + vector_ranks[pid])) +
                self.weights["keyword"] * (1 / (self.k + keyword_ranks[pid])) +
                self.weights["graph"] * (1 / (self.k + graph_ranks[pid])) +
                self.weights["intent"] * (1 / (self.k + intent_ranks[pid]))
            )
        
        sorted_pids = sorted(rrf_scores.keys(), key=lambda x: rrf_scores[x], reverse=True)
        
        # Create a map of problem_id to similarity_score
        similarity_map = {c.problem_id: c.similarity_score for c in candidates}
        
        results = []
        for pid in sorted_pids:
            candidate = next(c for c in candidates if c.problem_id == pid)
            context = context_map.get(pid)
            
            # Add similarity_score to context if exists
            if context:
                context.similarity_score = similarity_map.get(pid, 0.0)
            
            results.append(RankedResult(
                problem_id=pid,
                rrf_score=rrf_scores[pid],
                vector_rank=vector_ranks[pid],
                keyword_rank=keyword_ranks[pid],
                graph_rank=graph_ranks[pid],
                intent_rank=intent_ranks[pid],
                context=context,
                similarity_score=similarity_map.get(pid, 0.0)  # For fast-path decision
            ))
        
        confidence_metrics = self._compute_confidence(results, query)
        
        # Calculate score_gap - khoảng cách giữa top 1 và top 2
        if len(results) > 1 and results[0].rrf_score > 0:
            raw_gap = results[0].rrf_score - results[1].rrf_score
            normalized_gap = raw_gap / results[0].rrf_score
            
            # Vector similarity gap
            if len(candidates) > 1:
                top_similarity = candidates[0].similarity_score
                second_similarity = candidates[1].similarity_score
                vector_gap = top_similarity - second_similarity
            else:
                vector_gap = 0.5
            
            # Use the maximum of normalized RRF gap and vector gap
            score_gap = max(normalized_gap, vector_gap)
            
            # CRITICAL: Nếu top similarity quá thấp, giảm score_gap
            # Vì similarity thấp nghĩa là không tìm được câu trả lời thực sự phù hợp
            top_similarity = candidates[0].similarity_score
            if top_similarity < 0.6:  # Similarity dưới 60% = không đủ tốt
                # Giảm score_gap theo tỉ lệ similarity
                penalty = top_similarity / 0.6  # 0.5 → penalty=0.83, 0.4 → penalty=0.67
                score_gap = score_gap * penalty
                logger.info(f"Low similarity penalty applied: gap {score_gap:.3f} (similarity={top_similarity:.2f})")
        else:
            score_gap = 1.0
        
        # Determine if results are ambiguous
        is_ambiguous = (
            confidence_metrics.gap_component < Config.SCORE_GAP_THRESHOLD or
            (len(candidates) > 0 and candidates[0].similarity_score < 0.55)  # Similarity quá thấp
        )
        
        return RankingOutput(
            results=results,
            confidence_score=confidence_metrics.final_score,
            score_gap=score_gap,
            is_ambiguous=is_ambiguous
        )
    
    def _get_vector_scores(self, candidates: List[CandidateProblem]) -> Dict[str, float]:
        return {c.problem_id: c.similarity_score for c in candidates}
    
    def _get_keyword_scores(self, candidates: List[CandidateProblem], query_text: str) -> Dict[str, float]:
        return {c.problem_id: self.keyword_matcher.score_candidate(query_text, c) for c in candidates}
    
    def _get_graph_scores(self, candidates: List[CandidateProblem], context_map: Dict[str, RetrievedContext], query: StructuredQueryObject) -> Dict[str, float]:
        return {c.problem_id: self.graph_scorer.score(c, context_map.get(c.problem_id), query) for c in candidates}
    
    def _get_intent_scores(self, candidates: List[CandidateProblem], query: StructuredQueryObject) -> Dict[str, float]:
        return {c.problem_id: self.intent_scorer.score(c, query) for c in candidates}
    
    def _scores_to_ranks(self, scores: Dict[str, float]) -> Dict[str, int]:
        sorted_items = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return {pid: rank + 1 for rank, (pid, _) in enumerate(sorted_items)}
    

    
    
    def _compute_confidence(self, results: List[RankedResult], query: StructuredQueryObject) -> ConfidenceMetrics:
        if not results:
            return ConfidenceMetrics(final_score=0.0, rrf_component=0.0, intent_component=0.0, gap_component=0.0, slot_component=0.0)
        
        max_rrf = sum(self.weights.values()) * (1 / (self.k + 1))
        rrf_confidence = results[0].rrf_score / max_rrf if max_rrf > 0 else 0
        intent_confidence = query.confidence_intent
        
        if len(results) > 1:
            gap = results[0].rrf_score - results[1].rrf_score
        else:
            gap = 0.2
        gap_confidence = min(gap / 0.20, 1.0)
        
        slot_penalty = 1.0 - (len(query.missing_slots) * 0.15)
        slot_penalty = max(slot_penalty, 0.4)
        
        final_confidence = (
            rrf_confidence * 0.35 +
            intent_confidence * 0.30 +
            gap_confidence * 0.20 +
            slot_penalty * 0.15
        )
        
        return ConfidenceMetrics(
            final_score=final_confidence,
            rrf_component=rrf_confidence,
            intent_component=intent_confidence,
            gap_component=gap,
            slot_component=slot_penalty
        )
