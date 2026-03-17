import logging
from typing import Optional

from schema import (
    StructuredQueryObject,
    RankingOutput,
    Decision,
    DecisionType,
    Config,
)

logger = logging.getLogger(__name__)


class DecisionEngine:
    """ định tuyến quyết định dựa trên độ tin cậy và trạng thái."""
    
    def __init__(self):
        self.conf_high = Config.CONFIDENCE_HIGH_THRESHOLD
        self.conf_medium = Config.CONFIDENCE_MEDIUM_THRESHOLD
        self.conf_low = Config.CONFIDENCE_LOW_THRESHOLD
        self.gap_threshold = Config.SCORE_GAP_THRESHOLD
        self.max_clarify = Config.MAX_CLARIFY_COUNT
    
    def decide(
        self,
        query: StructuredQueryObject,
        ranking: RankingOutput,
        clarify_count: int = 0
    ) -> Decision:
        """Ra quyết định dựa trên query, ranking và trạng thái phiên."""
        logger.info(
            f"Decision input: confidence={ranking.confidence_score:.2f}, "
            f"gap={ranking.score_gap:.2f}, "
            f"is_ambiguous={ranking.is_ambiguous}, "
            f"need_account={query.need_account_lookup}, "
            f"out_of_domain={query.is_out_of_domain}, "
            f"clarify_count={clarify_count}"
        )
        
        # === Early exits - ONLY for out of domain ===
        # NOTE: need_account_lookup is now handled differently
        # We still do retrieval and provide guidance, then append escalation info
        
        if query.is_out_of_domain:
            logger.info("Decision: ESCALATE_OUT_OF_SCOPE")
            return Decision(
                type=DecisionType.ESCALATE_OUT_OF_SCOPE,
                escalation_reason="Câu hỏi ngoài phạm vi hỗ trợ"
            )
        
        if clarify_count >= self.max_clarify:
            logger.info(f"Decision: ESCALATE_MAX_RETRY (count={clarify_count})")
            return Decision(
                type=DecisionType.ESCALATE_MAX_RETRY,
                escalation_reason=f"Đã hỏi lại {clarify_count} lần"
            )
        
        if not ranking.results:
            # If no results AND need account lookup -> escalate to personal
            if query.need_account_lookup:
                logger.info("Decision: ESCALATE_PERSONAL (no results + need account)")
                return Decision(
                    type=DecisionType.ESCALATE_PERSONAL,
                    escalation_reason="Cần truy cập thông tin giao dịch cá nhân"
                )
            logger.info("Decision: ESCALATE_LOW_CONFIDENCE (no results)")
            return Decision(
                type=DecisionType.ESCALATE_LOW_CONFIDENCE,
                escalation_reason="Không tìm thấy thông tin phù hợp"
            )
        
        # === CRITICAL FIX: High similarity override ===
        # Nếu top result có similarity rất cao (>= 0.95), đây là exact/near-exact match
        # Trả lời trực tiếp bất kể gap (vì có thể có duplicate entries)
        top_result = ranking.results[0]
        top_similarity = top_result.context.similarity_score if top_result.context else 0
        
        # Fallback: check từ candidates nếu context không có
        if top_similarity == 0 and hasattr(ranking, 'results') and ranking.results:
            # Similarity thường được lưu trong context
            pass
        
        if top_similarity >= 0.95:
            logger.info(f"Decision: DIRECT_ANSWER (high similarity={top_similarity:.3f} - exact match)")
            return Decision(
                type=DecisionType.DIRECT_ANSWER,
                top_result=top_result
            )
        
        # === CRITICAL: Intelligent Decision Making ===
        # Kết hợp nhiều yếu tố để quyết định:
        # 1. Confidence score (từ ranking algorithm)
        # 2. Score gap (khoảng cách giữa top 1 và top 2)
        # 3. Top result RRF score (quality của kết quả tốt nhất)
        
        top_rrf = ranking.results[0].rrf_score if ranking.results else 0
        
        # === IMPORTANT: Confidence-first approach ===
        # Nếu confidence đủ cao (>= 0.65), trả lời trực tiếp
        # Gap thấp không có nghĩa là kết quả sai, có thể chỉ là nhiều câu hỏi tương tự
        if ranking.confidence_score >= 0.65:
            logger.info(f"Decision: DIRECT_ANSWER (high confidence={ranking.confidence_score:.3f})")
            return Decision(
                type=DecisionType.DIRECT_ANSWER,
                top_result=ranking.results[0]
            )
        
        # Tính "certainty score" - độ chắc chắn tổng hợp
        # Confidence cao + Gap cao = Rất chắc chắn
        # Confidence cao + Gap thấp = Có thể trả lời nhưng cần thận trọng
        # Confidence thấp = Không chắc chắn
        
        # Normalize gap: 0.15 -> 1.0, 0.0 -> 0.0
        normalized_gap = min(ranking.score_gap / self.gap_threshold, 1.0)
        
        # Tính certainty score - ĐÃ ĐIỀU CHỈNH: giảm trọng số của gap
        certainty = (
            ranking.confidence_score * 0.75 +  # Confidence là yếu tố CHÍNH (tăng từ 0.6)
            normalized_gap * 0.15 +             # Gap ít quan trọng hơn (giảm từ 0.3)
            min(top_rrf * 2, 1.0) * 0.10        # RRF score boost
        )
        
        logger.info(
            f"Certainty calculation: conf={ranking.confidence_score:.3f}, "
            f"gap={ranking.score_gap:.3f}, normalized_gap={normalized_gap:.3f}, "
            f"rrf={top_rrf:.3f}, certainty={certainty:.3f}"
        )
        
        # Decision thresholds 
        CERTAINTY_HIGH = 0.55     
        CERTAINTY_MEDIUM = 0.45   
        CERTAINTY_LOW = 0.35      
        
        
        if certainty < CERTAINTY_LOW:
            logger.info(
                f"Decision: ESCALATE_LOW_CONFIDENCE "
                f"(certainty={certainty:.3f} < {CERTAINTY_LOW})"
            )
            return Decision(
                type=DecisionType.ESCALATE_LOW_CONFIDENCE,
                escalation_reason="Không tìm thấy câu trả lời phù hợp rõ ràng với câu hỏi của bạn"
            )
        
        # === Decision based on certainty (higher priority than ambiguous check) ===
        if certainty >= CERTAINTY_HIGH:
            logger.info(f"Decision: DIRECT_ANSWER (certainty={certainty:.3f})")
            return Decision(
                type=DecisionType.DIRECT_ANSWER,
                top_result=ranking.results[0]
            )
        
        if certainty >= CERTAINTY_MEDIUM:
            logger.info(f"Decision: ANSWER_WITH_CLARIFY (certainty={certainty:.3f})")
            return Decision(
                type=DecisionType.ANSWER_WITH_CLARIFY,
                top_result=ranking.results[0],
                clarification_slots=query.missing_slots[:2] if query.missing_slots else []
            )
        
        # === Check ambiguous results (only for low certainty) ===
        if ranking.is_ambiguous:
            logger.info(f"Decision: CLARIFY_REQUIRED (ambiguous results, certainty={certainty:.3f})")
            return Decision(
                type=DecisionType.CLARIFY_REQUIRED,
                clarification_slots=query.missing_slots[:2] if query.missing_slots else ["chi tiết câu hỏi"]
            )
        
        # certainty >= CERTAINTY_LOW but < CERTAINTY_MEDIUM and not ambiguous
        if query.missing_slots:
            logger.info(f"Decision: CLARIFY_REQUIRED (certainty={certainty:.3f})")
            return Decision(
                type=DecisionType.CLARIFY_REQUIRED,
                clarification_slots=query.missing_slots[:2]
            )
        
        # Fallback: answer with clarify for borderline cases
        logger.info(f"Decision: ANSWER_WITH_CLARIFY (borderline, certainty={certainty:.3f})")
        return Decision(
            type=DecisionType.ANSWER_WITH_CLARIFY,
            top_result=ranking.results[0],
            clarification_slots=["Thông tin này có đúng với bạn không?"]
        )
    
    def get_decision_explanation(self, decision: Decision) -> str:
        """Lấy giải thích cho quyết định"""
        explanations = {
            DecisionType.DIRECT_ANSWER: "Độ tin cậy cao -> Trả lời trực tiếp",
            DecisionType.ANSWER_WITH_CLARIFY: "Độ tin cậy trung bình -> Trả lời kèm câu hỏi",
            DecisionType.CLARIFY_REQUIRED: "Cần thêm thông tin -> Hỏi làm rõ",
            DecisionType.ESCALATE_PERSONAL: "Cần kiểm tra dữ liệu cá nhân -> Chuyển tổng đài",
            DecisionType.ESCALATE_OUT_OF_SCOPE: "Ngoài phạm vi -> Từ chối lịch sự",
            DecisionType.ESCALATE_MAX_RETRY: "Hỏi lại quá nhiều -> Chuyển tổng đài",
            DecisionType.ESCALATE_LOW_CONFIDENCE: "Độ tin cậy thấp -> Chuyển tổng đài",
        }
        return explanations.get(decision.type, "Unknown")


class SessionManager:
    """Quản lý trạng thái phiên bao gồm đếm số lần hỏi lại"""
    
    def __init__(self, redis_client=None):
        self.redis = redis_client
        self._redis_available = False
        self._local_store = {}
        self.ttl = Config.SESSION_TTL_SECONDS
        
        if self.redis:
            try:
                self.redis.ping()
                self._redis_available = True
            except Exception:
                self._redis_available = False
    
    def get_clarify_count(self, session_id: str) -> int:
        if self._redis_available:
            try:
                count = self.redis.get(f"clarify:{session_id}")
                return int(count) if count else 0
            except Exception:
                self._redis_available = False
        return self._local_store.get(f"clarify:{session_id}", 0)
    
    def increment_clarify_count(self, session_id: str) -> int:
        key = f"clarify:{session_id}"
        if self._redis_available:
            try:
                count = self.redis.incr(key)
                self.redis.expire(key, self.ttl)
                return int(count)
            except Exception:
                self._redis_available = False
        current = self._local_store.get(key, 0)
        self._local_store[key] = current + 1
        return current + 1
    
    def reset_clarify_count(self, session_id: str) -> None:
        key = f"clarify:{session_id}"
        if self._redis_available:
            try:
                self.redis.delete(key)
                return
            except Exception:
                self._redis_available = False
        self._local_store.pop(key, None)
    
    def should_increment_clarify(self, decision: Decision) -> bool:
        return decision.type == DecisionType.CLARIFY_REQUIRED
    
    def should_reset_clarify(self, decision: Decision) -> bool:
        return decision.type in [
            DecisionType.DIRECT_ANSWER,
            DecisionType.ANSWER_WITH_CLARIFY
        ]
