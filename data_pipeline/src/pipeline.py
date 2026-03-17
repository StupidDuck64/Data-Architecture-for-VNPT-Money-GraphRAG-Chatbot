"""
Pipeline orchestrating all components:
1. Intent Parsing
2. Constraint-based Retrieval
3. Multi-signal Ranking
4. Decision Engine
5. Response Generation
6. Logging
7. Rate Limiting
8. Monitoring
"""

import logging
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import asdict
import json

from schema import (
    Message,
    StructuredQueryObject,
    FormattedResponse,
    InteractionLog,
    DecisionType,
    Config,
)
from intent_parser import IntentParser, IntentParserLocal
from retrieval import RetrievalPipeline
from ranking import MultiSignalRanker
from decision_engine import DecisionEngine, SessionManager
from response_generator import ResponseGenerator, ResponseGeneratorSimple

# New modules
try:
    from redis_manager import RedisManager, get_redis_manager, init_redis
    from monitoring import MonitoringDashboard, get_monitoring_dashboard, init_monitoring
    ADVANCED_FEATURES_AVAILABLE = True
except ImportError as e:
    ADVANCED_FEATURES_AVAILABLE = False
    logging.warning(f"Advanced features not available: {e}")

logger = logging.getLogger(__name__)


class ChatbotPipeline:
    """
    Main chatbot pipeline orchestrating all components.
    
    Flow:
    1. Input Processing (history, validation)
    2. Intent Parsing → StructuredQueryObject
    3. Retrieval → Candidates + Contexts
    4. Ranking → RankingOutput
    5. Decision → Decision
    6. Response Generation → FormattedResponse
    7. Logging
    """
    
    def __init__(
        self,
        neo4j_driver,
        llm_client,
        embedding_client,
        redis_client=None,
        use_llm_parser: bool = True,
        use_llm_generator: bool = True,
        enable_monitoring: bool = True
    ):
        """
        Initialize all pipeline components.
        
        Args:
            neo4j_driver: Neo4j driver
            llm_client: OpenAI or compatible LLM client
            embedding_client: Embedding client
            redis_client: Optional Redis for session management
            use_llm_parser: Use LLM for intent parsing (vs rule-based)
            use_llm_generator: Use LLM for response generation (vs templates)
            enable_monitoring: Enable monitoring dashboard
        """
        # Store references
        self.neo4j_driver = neo4j_driver
        self.llm_client = llm_client
        
        # Core components
        self.retrieval = RetrievalPipeline(neo4j_driver, embedding_client)
        self.ranker = MultiSignalRanker()
        self.decision_engine = DecisionEngine()
        self.session_manager = SessionManager(redis_client)
        
        # Advanced features
        self.monitoring = None
        
        if ADVANCED_FEATURES_AVAILABLE:
            # Initialize Monitoring
            if enable_monitoring:
                self.monitoring = init_monitoring(neo4j_driver, llm_client)
                logger.info("Monitoring enabled")
        
        # LLM-dependent components
        if use_llm_parser:
            self.intent_parser = IntentParser(llm_client)
        else:
            self.intent_parser = IntentParserLocal()
        
        if use_llm_generator:
            self.response_generator = ResponseGenerator(llm_client)
        else:
            self.response_generator = ResponseGeneratorSimple()
        
        # Chat history storage
        self._chat_histories = {}  # session_id -> List[Message]
    
    def process(
        self,
        user_message: str,
        session_id: str
    ) -> FormattedResponse:
        """
        Process a user message through the full pipeline.
        
        Args:
            user_message: User's input message
            session_id: Session identifier
            
        Returns:
            FormattedResponse with answer and metadata
        """
        start_time = time.time()
        
        # Initialize log entry
        log_entry = self._init_log_entry(session_id, user_message)
        
        try:
            # Step 1: Get chat history
            chat_history = self._get_chat_history(session_id)
            log_entry.chat_history_length = len(chat_history)
            
            # Step 2: Intent Parsing
            parse_start = time.time()
            query = self.intent_parser.parse(user_message, chat_history)
            log_entry.intent_parse_latency_ms = int((time.time() - parse_start) * 1000)
            log_entry.structured_query = query
            
            logger.info(f"Parsed intent: service={query.service.value}, "
                       f"problem={query.problem_type.value}, "
                       f"confidence={query.confidence_intent:.2f}")
            
            # Check for out of domain - early exit only for truly unrelated questions
            if query.is_out_of_domain:
                return self._handle_early_exit(query, log_entry, start_time, session_id, user_message)
            
            # For need_account_lookup: still do retrieval to provide helpful guidance
            # The response will include both guidance AND escalation info
            
            # Step 3: Retrieval (use fallback for better coverage)
            retrieval_start = time.time()
            candidates, contexts = self.retrieval.retrieve_with_fallback(query)
            log_entry.retrieval_latency_ms = int((time.time() - retrieval_start) * 1000)
            log_entry.constrained_problem_count = len(candidates)
            log_entry.retrieval_candidates = [
                {"problem_id": c.problem_id, "similarity": c.similarity_score}
                for c in candidates
            ]
            
            logger.info(f"Retrieved {len(candidates)} candidates")
            
            # Step 4: Ranking
            ranking_start = time.time()
            ranking_output = self.ranker.rank(candidates, contexts, query)
            log_entry.ranking_latency_ms = int((time.time() - ranking_start) * 1000)
            log_entry.confidence_score = ranking_output.confidence_score
            log_entry.score_gap = ranking_output.score_gap
            log_entry.is_ambiguous = ranking_output.is_ambiguous
            log_entry.rrf_scores = [
                {"problem_id": r.problem_id, "rrf_score": r.rrf_score}
                for r in ranking_output.results[:5]
            ]
            
            logger.info(f"Ranking: confidence={ranking_output.confidence_score:.2f}, "
                       f"gap={ranking_output.score_gap:.2f}")
            
            # Step 5: Decision
            clarify_count = self.session_manager.get_clarify_count(session_id)
            decision = self.decision_engine.decide(query, ranking_output, clarify_count)
            
            log_entry.decision_type = decision.type
            log_entry.clarification_slots = decision.clarification_slots
            log_entry.escalation_reason = decision.escalation_reason
            
            if decision.top_result:
                log_entry.selected_problem_id = decision.top_result.problem_id
                if decision.top_result.context:
                    log_entry.selected_answer_id = decision.top_result.context.answer_id
            
            logger.info(f"Decision: {decision.type.value}")
            
            # Update session state
            self._update_session_state(session_id, decision)
            
            # Step 6: Response Generation
            response_start = time.time()
            context = decision.top_result.context if decision.top_result else None
            
            # Collect all contexts from top results for LLM synthesis
            all_contexts = []
            if ranking_output.results:
                for result in ranking_output.results[:3]:  # Top 3 results (reduced from 5 for speed)
                    if result.context:
                        all_contexts.append(result.context)
            
            response = self.response_generator.generate(
                decision, context, user_message, 
                all_contexts=all_contexts,
                need_account_lookup=query.need_account_lookup
            )
            log_entry.response_latency_ms = int((time.time() - response_start) * 1000)
            log_entry.final_response = response.message
            log_entry.source_citation = response.source_citation
            
            # Finalize log
            log_entry.total_latency_ms = int((time.time() - start_time) * 1000)
            self._save_log(log_entry)
            
            # Update chat history
            self._update_chat_history(session_id, user_message, response.message)
            
            # Record metrics to monitoring dashboard
            if self.monitoring:
                total_latency = log_entry.total_latency_ms
                # Increment total counter (for dashboard)
                self.monitoring.metrics.increment("requests_total")
                # Increment counter with decision label (for detailed stats)
                self.monitoring.metrics.increment(f"decision_{decision.type.value}")
                self.monitoring.metrics.observe("request_latency_ms", total_latency)
                self.monitoring.metrics.observe("confidence_score", ranking_output.confidence_score)
            
            return response
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            log_entry.total_latency_ms = int((time.time() - start_time) * 1000)
            self._save_log(log_entry)
            
            # Record error in monitoring
            if self.monitoring:
                self.monitoring.metrics.increment("errors_total")
            
            # Return fallback response
            return FormattedResponse(
                message="Xin lỗi, đã có lỗi xảy ra. Vui lòng thử lại hoặc liên hệ hotline 18001091 (nhánh 3) để được hỗ trợ.",
                source_citation="",
                decision_type=DecisionType.ESCALATE_LOW_CONFIDENCE
            )
    
    def _handle_early_exit(
        self,
        query: StructuredQueryObject,
        log_entry: InteractionLog,
        start_time: float,
        session_id: str,
        user_message: str
    ) -> FormattedResponse:
        """Handle early exit cases (personal data, out of domain)."""
        
        # Create minimal decision
        from schema import RankingOutput, Decision
        
        if query.need_account_lookup:
            decision = Decision(
                type=DecisionType.ESCALATE_PERSONAL,
                escalation_reason="Cần truy cập thông tin giao dịch cá nhân"
            )
        else:
            decision = Decision(
                type=DecisionType.ESCALATE_OUT_OF_SCOPE,
                escalation_reason="Câu hỏi ngoài phạm vi hỗ trợ"
            )
        
        log_entry.decision_type = decision.type
        log_entry.escalation_reason = decision.escalation_reason
        log_entry.constrained_problem_count = 0
        log_entry.confidence_score = 0.0
        log_entry.score_gap = 0.0
        log_entry.is_ambiguous = False
        
        response_start = time.time()
        response = self.response_generator.generate(decision, None, user_message)
        log_entry.response_latency_ms = int((time.time() - response_start) * 1000)
        log_entry.final_response = response.message
        log_entry.source_citation = response.source_citation
        log_entry.total_latency_ms = int((time.time() - start_time) * 1000)
        
        self._save_log(log_entry)
        self._update_chat_history(session_id, user_message, response.message)
        
        return response
    
    def _get_chat_history(self, session_id: str) -> List[Message]:
        """
        Get chat history for session.
        
        Prioritizes Redis for persistence, falls back to in-memory.
        """
        # Try Redis first
        if ADVANCED_FEATURES_AVAILABLE:
            try:
                redis_mgr = get_redis_manager()
                if redis_mgr and redis_mgr.is_connected:
                    history_data = redis_mgr.get_chat_history(
                        session_id, 
                        max_messages=Config.CHAT_HISTORY_MAX_MESSAGES
                    )
                    if history_data:
                        return [Message(role=m["role"], content=m["content"]) for m in history_data]
            except Exception as e:
                logger.warning(f"Redis chat history fetch failed: {e}, using in-memory")
        
        # Fallback to in-memory
        history = self._chat_histories.get(session_id, [])
        return history[-Config.CHAT_HISTORY_MAX_MESSAGES:]
    
    def _update_chat_history(
        self, 
        session_id: str, 
        user_message: str, 
        assistant_message: str
    ) -> None:
        """
        Update chat history.
        
        Stores in both Redis (for persistence) and in-memory (for fast access).
        """
        # Try Redis first
        if ADVANCED_FEATURES_AVAILABLE:
            try:
                redis_mgr = get_redis_manager()
                if redis_mgr and redis_mgr.is_connected:
                    redis_mgr.update_chat_history(session_id, user_message, assistant_message)
                    logger.debug(f"Chat history saved to Redis for session {session_id}")
            except Exception as e:
                logger.warning(f"Redis chat history update failed: {e}")
        
        # Also update in-memory (fallback + fast access)
        if session_id not in self._chat_histories:
            self._chat_histories[session_id] = []
        
        self._chat_histories[session_id].append(
            Message(role="user", content=user_message)
        )
        self._chat_histories[session_id].append(
            Message(role="assistant", content=assistant_message)
        )
        
        # Trim to max length
        max_messages = Config.CHAT_HISTORY_MAX_MESSAGES * 2  # pairs
        if len(self._chat_histories[session_id]) > max_messages:
            self._chat_histories[session_id] = self._chat_histories[session_id][-max_messages:]
    
    def _update_session_state(self, session_id: str, decision) -> None:
        """Update session state based on decision."""
        if self.session_manager.should_increment_clarify(decision):
            self.session_manager.increment_clarify_count(session_id)
        elif self.session_manager.should_reset_clarify(decision):
            self.session_manager.reset_clarify_count(session_id)
    
    def _init_log_entry(self, session_id: str, user_message: str) -> InteractionLog:
        """Initialize a log entry."""
        history = self._chat_histories.get(session_id, [])
        turn_number = len([m for m in history if m.role == "user"]) + 1
        
        return InteractionLog(
            session_id=session_id,
            timestamp=datetime.now(),
            turn_number=turn_number,
            user_message=user_message,
            chat_history_length=0,
            structured_query=None,
            intent_parse_latency_ms=0,
            constrained_problem_count=0,
            retrieval_candidates=[],
            retrieval_latency_ms=0,
            rrf_scores=[],
            confidence_score=0.0,
            score_gap=0.0,
            is_ambiguous=False,
            ranking_latency_ms=0,
            decision_type=DecisionType.ESCALATE_LOW_CONFIDENCE,
            selected_problem_id=None,
            selected_answer_id=None,
            clarification_slots=[],
            escalation_reason=None,
            final_response="",
            response_latency_ms=0,
            source_citation="",
            total_latency_ms=0
        )
    
    def _save_log(self, log_entry: InteractionLog) -> None:
        """Save interaction log."""
        # Convert to dict for logging
        log_dict = {
            "session_id": log_entry.session_id,
            "timestamp": log_entry.timestamp.isoformat(),
            "turn_number": log_entry.turn_number,
            "user_message": log_entry.user_message[:100] + "..." if len(log_entry.user_message) > 100 else log_entry.user_message,
            "decision_type": log_entry.decision_type.value,
            "confidence_score": log_entry.confidence_score,
            "total_latency_ms": log_entry.total_latency_ms,
            "selected_problem_id": log_entry.selected_problem_id,
        }
        
        logger.info(f"Interaction log: {json.dumps(log_dict, ensure_ascii=False)}")
        
        # TODO: Save to database or analytics service
    
    def clear_session(self, session_id: str) -> None:
        """Clear session data."""
        self._chat_histories.pop(session_id, None)
        self.session_manager.reset_clarify_count(session_id)


def create_pipeline(
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    openai_api_key: str,
    redis_url: Optional[str] = None,
    use_llm: bool = True,
    enable_monitoring: bool = True
) -> ChatbotPipeline:
    """
    Factory function to create pipeline with all dependencies.
    
    Args:
        neo4j_uri: Neo4j connection URI
        neo4j_user: Neo4j username
        neo4j_password: Neo4j password
        openai_api_key: OpenAI API key
        redis_url: Optional Redis URL for session management
        use_llm: Whether to use LLM (vs rule-based fallbacks)
        enable_monitoring: Enable monitoring dashboard
        
    Returns:
        Configured ChatbotPipeline
    """
    from neo4j import GraphDatabase
    from openai import OpenAI
    
    # Create Neo4j driver
    neo4j_driver = GraphDatabase.driver(
        neo4j_uri,
        auth=(neo4j_user, neo4j_password)
    )
    
    # Create OpenAI client
    llm_client = OpenAI(api_key=openai_api_key)
    embedding_client = llm_client  # Same client for embeddings
    
    # Create Redis client if URL provided
    redis_client = None
    if redis_url:
        try:
            import redis
            redis_client = redis.from_url(redis_url)
            
            # Initialize Redis manager for advanced features
            if ADVANCED_FEATURES_AVAILABLE:
                init_redis(redis_url)
                logger.info("Redis manager initialized for advanced features")
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
    
    return ChatbotPipeline(
        neo4j_driver=neo4j_driver,
        llm_client=llm_client,
        embedding_client=embedding_client,
        redis_client=redis_client,
        use_llm_parser=use_llm,
        use_llm_generator=use_llm,
        enable_monitoring=enable_monitoring
    )
