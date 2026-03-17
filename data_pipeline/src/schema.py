"""
VNPT Money GraphRAG Chatbot - Schema & Constants
=================================================

This module defines all enums, dataclasses, and constants used across the system.
All values are strictly controlled - no free-form generation allowed.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any
from datetime import datetime


# ==============================================================================
# ENUMS
# ==============================================================================

class ServiceEnum(str, Enum):
    """
    Enumeration of all supported services.
    Used to constrain graph search space.
    
    Mapped from actual data in nodes_topic.csv and nodes_problem.csv
    """
    # === Dịch vụ tài chính cơ bản (ho_tro_khach_hang) ===
    NAP_TIEN = "nap_tien"              # nap_dien_thoai, nap_tien_mobile_money
    RUT_TIEN = "rut_tien"              # rut_tien_mobile_money
    CHUYEN_TIEN = "chuyen_tien"        # chuyen_tien_mobile_money
    LIEN_KET_NGAN_HANG = "lien_ket_ngan_hang"  # lien_ket_ngan_hang
    THANH_TOAN = "thanh_toan"          # thanh_toan_dich_vu, thanh_toan_tu_dong
    
    # === Tài khoản & Bảo mật (ho_tro_khach_hang) ===
    OTP = "otp"                        # SmartOTP
    HAN_MUC = "han_muc"                # han_muc_giao_dich
    DANG_KY = "dang_ky"                # dang_ky_tai_khoan, dang_ky_mobile_money
    DINH_DANH = "dinh_danh"            # dinh_danh_ekyc
    BAO_MAT = "bao_mat"                # bao_mat_thong_tin
    
    # === Dịch vụ viễn thông (dich_vu) ===
    DATA_3G_4G = "data_3g_4g"          # data_3g4g - Gói data, gói cước
    MUA_THE = "mua_the"                # mua_ma_the - Mua thẻ điện thoại, mã thẻ
    DI_DONG_TRA_SAU = "di_dong_tra_sau"  # di_dong_tra_sau - Cước trả sau
    HOA_DON_VIEN_THONG = "hoa_don_vien_thong"  # hoa_don_vien_thong
    
    # === Tiền điện nước (dich_vu) ===
    TIEN_DIEN = "tien_dien"            # tien_dien
    TIEN_NUOC = "tien_nuoc"            # tien_nuoc
    DIEN_NUOC_KHAC = "dien_nuoc_khac"  # dien_nuoc_khac, phi_chung_cu
    
    # === Tài chính - Bảo hiểm - Vay (dich_vu) ===
    BAO_HIEM = "bao_hiem"              # bao_hiem_so, bao_hiem_vietinbank, manulife
    VAY = "vay"                        # vay, fe_credit, msb_credit, aeon_finance
    TIET_KIEM = "tiet_kiem"            # tiet_kiem_online
    
    # === Học phí (dich_vu) ===
    HOC_PHI = "hoc_phi"                # hoc_phi, hoc_phi_vnedu, hoc_phi_ssc, etc.
    
    # === Vé & Đặt chỗ (dich_vu) ===
    MUA_VE = "mua_ve"                  # mua_ve_tau, mua_ve_may_bay, ve_may_bay, dat_phong_khach_san
    
    # === Dịch vụ công (dich_vu) ===
    DICH_VU_CONG = "dich_vu_cong"      # nop_phat_giao_thong, nop_thue_le_phi_truoc_ba, dong_bhyt_bhxh
    
    # === Giải trí (dich_vu) ===
    GIAI_TRI = "giai_tri"              # mytv, vtvcab, truyen_hinh_k, vietlott, vong_quay
    
    # === Ứng dụng VNPT Money ===
    UNG_DUNG = "ung_dung"              # tai_va_cap_nhat_ung_dung
    
    # === Điều khoản & Quyền riêng tư (dieu_khoan, quyen_rieng_tu) ===
    DIEU_KHOAN = "dieu_khoan"          # All topics in dieu_khoan group
    QUYEN_RIENG_TU = "quyen_rieng_tu"  # All topics in quyen_rieng_tu group
    
    # === Fallback ===
    KHAC = "khac"


class ProblemTypeEnum(str, Enum):
    """
    Enumeration of problem types.
    Helps narrow down the specific issue.
    """
    KHONG_NHAN_OTP = "khong_nhan_otp"
    THAT_BAI = "that_bai"
    PENDING_LAU = "pending_lau"
    VUOT_HAN_MUC = "vuot_han_muc"
    TRU_TIEN_CHUA_NHAN = "tru_tien_chua_nhan"
    LOI_KET_NOI = "loi_ket_noi"
    HUONG_DAN = "huong_dan"
    CHINH_SACH = "chinh_sach"
    KHAC = "khac"


class DecisionType(str, Enum):
    """
    Decision outcomes from the Decision Engine.
    Each type has a specific response template.
    """
    DIRECT_ANSWER = "direct_answer"
    ANSWER_WITH_CLARIFY = "answer_with_clarify"
    CLARIFY_REQUIRED = "clarify_required"
    ESCALATE_PERSONAL = "escalate_personal"
    ESCALATE_OUT_OF_SCOPE = "escalate_out_of_scope"
    ESCALATE_MAX_RETRY = "escalate_max_retry"
    ESCALATE_LOW_CONFIDENCE = "escalate_low_confidence"


# ==============================================================================
# DATA CLASSES
# ==============================================================================

@dataclass
class Message:
    """Represents a single chat message."""
    role: str  # "user" or "assistant"
    content: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class StructuredQueryObject:
    """
    The core structured query extracted from user input.
    This is the PRIMARY input for retrieval - NOT raw user message.
    
    LLM chỉ được dùng để populate object này, KHÔNG được sinh answer.
    """
    # Required fields
    service: ServiceEnum
    problem_type: ProblemTypeEnum
    condensed_query: str  # Normalized query for vector search
    
    # Optional refinements
    topic: Optional[str] = None
    bank: Optional[str] = None
    amount: Optional[float] = None
    error_code: Optional[str] = None
    
    # Critical flags
    need_account_lookup: bool = False  # TRUE → immediate escalation
    is_out_of_domain: bool = False     # TRUE → out of scope response
    
    # Confidence & clarity
    confidence_intent: float = 0.5     # 0.0 - 1.0
    missing_slots: List[str] = field(default_factory=list)
    
    # Raw input for logging
    original_message: str = ""


@dataclass
class CandidateProblem:
    """A retrieved Problem candidate with scoring signals."""
    problem_id: str
    title: str
    description: Optional[str]
    intent: Optional[str]
    keywords: List[str]
    similarity_score: float  # From vector search


@dataclass
class RetrievedContext:
    """Full context retrieved from graph traversal."""
    problem_id: str
    problem_title: str
    answer_id: str
    answer_content: str
    answer_steps: Optional[List[str]]
    answer_notes: Optional[str]
    topic_id: str
    topic_name: str
    group_id: str
    group_name: str
    similarity_score: float = 0.0  # Added for decision making


@dataclass
class RankedResult:
    """A single ranked result with all scoring signals."""
    problem_id: str
    rrf_score: float
    vector_rank: int
    keyword_rank: int
    graph_rank: int
    intent_rank: int
    context: Optional[RetrievedContext] = None
    similarity_score: float = 0.0  # For fast-path decision


@dataclass
class RankingOutput:
    """Output from the multi-signal ranker."""
    results: List[RankedResult]
    confidence_score: float
    score_gap: float
    is_ambiguous: bool


@dataclass
class Decision:
    """Decision from the Decision Engine."""
    type: DecisionType
    top_result: Optional[RankedResult] = None
    clarification_slots: List[str] = field(default_factory=list)
    escalation_reason: Optional[str] = None


@dataclass
class FormattedResponse:
    """Final formatted response to user."""
    message: str
    source_citation: str
    decision_type: DecisionType


@dataclass
class ConfidenceMetrics:
    """Detailed breakdown of confidence computation."""
    final_score: float
    rrf_component: float
    intent_component: float
    gap_component: float
    slot_component: float


# ==============================================================================
# CONSTANTS
# ==============================================================================

class Config:
    """
    Central configuration for the entire system.
    All thresholds and parameters defined here.
    """
    
    # === LLM Configuration ===
    INTENT_PARSER_MODEL = "gpt-4o-mini"
    INTENT_PARSER_TEMPERATURE = 0.0  # Deterministic
    INTENT_PARSER_MAX_TOKENS = 300  # Reduced for speed
    
    RESPONSE_GENERATOR_MODEL = "gpt-4o-mini"
    RESPONSE_GENERATOR_TEMPERATURE = 0.3  # Slight creativity for natural language
    RESPONSE_GENERATOR_MAX_TOKENS = 400  # Reduced for speed  
    # === Embedding ===
    EMBEDDING_MODEL = "text-embedding-3-small"
    EMBEDDING_DIMENSION = 1536
    
    # === Retrieval ===
    VECTOR_SEARCH_TOP_K = 10
    
    # === Ranking ===
    RRF_K = 60  # RRF parameter
    RANKING_WEIGHTS = {
        "vector": 1.0,
        "keyword": 0.8,
        "graph": 0.6,
        "intent": 1.2,
    }
    
    # === Decision Thresholds ===
    CONFIDENCE_HIGH_THRESHOLD = 0.85
    CONFIDENCE_MEDIUM_THRESHOLD = 0.60
    CONFIDENCE_LOW_THRESHOLD = 0.40
    SCORE_GAP_THRESHOLD = 0.15
    MAX_CLARIFY_COUNT = 10
    
    # === Session ===
    CHAT_HISTORY_MAX_MESSAGES = 10
    SESSION_TTL_SECONDS = 1800  # 30 minutes
    
    # === Logging ===
    LOG_SAMPLE_RATE_FOR_RAGAS = 0.10  # 10%


# ==============================================================================
# SERVICE → GROUP MAPPING (Deterministic)
# ==============================================================================

SERVICE_GROUP_MAP: Dict[str, List[str]] = {
    # ===========================================================================
    # MAPPING DỰA TRÊN PHÂN TÍCH DỮ LIỆU THỰC TẾ TỪ nodes_topic.csv
    # Mỗi service → List of groups chứa topics liên quan
    # ===========================================================================
    
    # === Dịch vụ tài chính cơ bản (chủ yếu trong ho_tro_khach_hang) ===
    "nap_tien": ["ho_tro_khach_hang", "dich_vu", "dieu_khoan"],
    "rut_tien": ["ho_tro_khach_hang", "dieu_khoan"],
    "chuyen_tien": ["ho_tro_khach_hang", "dieu_khoan"],
    "lien_ket_ngan_hang": ["ho_tro_khach_hang", "dieu_khoan"],
    "thanh_toan": ["ho_tro_khach_hang", "dich_vu", "dieu_khoan"],
    
    # === Tài khoản & Bảo mật ===
    "otp": ["ho_tro_khach_hang", "dieu_khoan"],
    "han_muc": ["dieu_khoan", "ho_tro_khach_hang"],
    "dang_ky": ["ho_tro_khach_hang", "dieu_khoan"],
    "dinh_danh": ["ho_tro_khach_hang", "dieu_khoan"],
    "bao_mat": ["quyen_rieng_tu", "ho_tro_khach_hang", "dieu_khoan"],
    
    # === Dịch vụ viễn thông (chủ yếu trong dich_vu) ===
    "data_3g_4g": ["dich_vu"],           # dich_vu__data_3g4g
    "mua_the": ["dich_vu"],              # dich_vu__mua_ma_the
    "di_dong_tra_sau": ["dich_vu"],      # dich_vu__di_dong_tra_sau
    "hoa_don_vien_thong": ["dich_vu"],   # dich_vu__hoa_don_vien_thong
    
    # === Tiền điện nước ===
    "tien_dien": ["dich_vu"],            # dich_vu__tien_dien
    "tien_nuoc": ["dich_vu"],            # dich_vu__tien_nuoc
    "dien_nuoc_khac": ["dich_vu"],       # dich_vu__dien_nuoc_khac, phi_chung_cu
    
    # === Tài chính - Bảo hiểm - Vay ===
    "bao_hiem": ["dich_vu"],             # bao_hiem_so, bao_hiem_vietinbank, manulife
    "vay": ["dich_vu"],                  # vay, fe_credit, msb_credit, aeon_finance, mirae_asset
    "tiet_kiem": ["dich_vu"],            # tiet_kiem_online
    
    # === Học phí ===
    "hoc_phi": ["dich_vu"],              # hoc_phi, hoc_phi_vnedu, hoc_phi_ssc, etc.
    
    # === Vé & Đặt chỗ ===
    "mua_ve": ["dich_vu"],               # mua_ve_tau, ve_may_bay, dat_phong_khach_san
    
    # === Dịch vụ công ===
    "dich_vu_cong": ["dich_vu"],         # nop_phat_giao_thong, nop_thue, dong_bhyt_bhxh
    
    # === Giải trí ===
    "giai_tri": ["dich_vu"],             # mytv, vtvcab, vietlott, vong_quay
    
    # === Ứng dụng VNPT Money ===
    "ung_dung": ["ho_tro_khach_hang"],   # tai_va_cap_nhat_ung_dung
    
    # === Điều khoản & Quyền riêng tư ===
    "dieu_khoan": ["dieu_khoan"],
    "quyen_rieng_tu": ["quyen_rieng_tu"],
    
    # === Fallback - tìm trong TẤT CẢ groups ===
    "khac": ["dich_vu", "ho_tro_khach_hang", "dieu_khoan", "quyen_rieng_tu"],
}


# ==============================================================================
# CLARIFICATION QUESTIONS
# ==============================================================================

CLARIFICATION_QUESTIONS: Dict[str, str] = {
    "service": "Bạn đang thực hiện giao dịch gì? (Ví dụ: nạp tiền, rút tiền, chuyển tiền, thanh toán hóa đơn...)",
    "problem_type": "Bạn gặp vấn đề gì cụ thể? (Ví dụ: không nhận được OTP, giao dịch thất bại, tiền bị trừ nhưng chưa nhận...)",
    "error_code": "Ứng dụng có hiển thị mã lỗi hoặc thông báo lỗi gì không?",
    "bank": "Bạn đang sử dụng ngân hàng nào để thực hiện giao dịch?",
    "amount": "Số tiền giao dịch của bạn là bao nhiêu?",
    "transaction_time": "Giao dịch này bạn thực hiện khi nào? (Ngày giờ cụ thể)",
}


# ==============================================================================
# ESCALATION TEMPLATES
# ==============================================================================

ESCALATION_TEMPLATES: Dict[str, str] = {
    "TEMPLATE_PERSONAL_DATA": """Để kiểm tra thông tin giao dịch cụ thể của bạn, mình cần chuyển yêu cầu đến bộ phận hỗ trợ.

📞 **Hotline**: 18001091 (nhánh 3)
📍 **Điểm giao dịch**: Các cửa hàng VinaPhone trên toàn quốc

Khi liên hệ, vui lòng cung cấp:
• Số điện thoại đăng ký VNPT Money
• Thời gian giao dịch
• Mã giao dịch (nếu có)

Tổng đài viên sẽ hỗ trợ kiểm tra ngay cho bạn.""",

    "TEMPLATE_OUT_OF_SCOPE": """Xin lỗi, câu hỏi này nằm ngoài phạm vi hỗ trợ của VNPT Money.

Mình có thể hỗ trợ bạn về:
• Nạp/rút tiền Mobile Money
• Chuyển tiền
• Thanh toán dịch vụ
• Liên kết ngân hàng
• Chính sách và điều khoản sử dụng

Bạn có câu hỏi nào khác về dịch vụ VNPT Money không?""",

    "TEMPLATE_MAX_RETRY": """Mình xin lỗi vì chưa hiểu đúng ý bạn.

Để được hỗ trợ tốt nhất, bạn có thể:
• Liên hệ hotline **18001091** (nhánh 3) để nói chuyện trực tiếp với tổng đài viên
• Hoặc thử đặt câu hỏi theo cách khác

Mình luôn sẵn sàng hỗ trợ bạn! 😊""",

    "TEMPLATE_LOW_CONFIDENCE": """Mình chưa tìm thấy thông tin phù hợp với câu hỏi của bạn.

Bạn có thể thử:
• Diễn đạt câu hỏi theo cách khác
• Hỏi về một chủ đề cụ thể hơn (ví dụ: nạp tiền, chuyển tiền, thanh toán...)
• Hoặc liên hệ hotline **18001091** (nhánh 3) để được hỗ trợ trực tiếp

Mình sẵn sàng giúp bạn! 😊""",

    "TEMPLATE_AMBIGUOUS": """Mình tìm thấy một số kết quả có thể liên quan đến câu hỏi của bạn:

{candidate_summaries}

Bạn có thể cho mình biết cụ thể hơn bạn muốn hỏi về vấn đề nào không?""",
}


# ==============================================================================
# FORBIDDEN PHRASES (Anti-hallucination)
# ==============================================================================

FORBIDDEN_PHRASES: List[str] = [
    "giao dịch của bạn đã thành công",
    "giao dịch của bạn đã thất bại",
    "tôi thấy trong hệ thống",
    "theo như tôi kiểm tra",
    "có thể giao dịch của bạn",
    "tôi nghĩ rằng giao dịch",
    "có lẽ tiền của bạn",
    "theo thông tin tôi có",
]


# ==============================================================================
# LOGGING SCHEMA
# ==============================================================================

@dataclass
class InteractionLog:
    """Complete log of a single interaction for audit and evaluation."""
    
    # Session info
    session_id: str
    timestamp: datetime
    turn_number: int
    
    # Input
    user_message: str
    chat_history_length: int
    
    # Intent Parsing
    structured_query: Optional[StructuredQueryObject]
    intent_parse_latency_ms: int
    
    # Retrieval
    constrained_problem_count: int
    retrieval_candidates: List[Dict[str, Any]]
    retrieval_latency_ms: int
    
    # Ranking
    rrf_scores: List[Dict[str, Any]]
    confidence_score: float
    score_gap: float
    is_ambiguous: bool
    ranking_latency_ms: int
    
    # Decision
    decision_type: DecisionType
    selected_problem_id: Optional[str]
    selected_answer_id: Optional[str]
    clarification_slots: List[str]
    escalation_reason: Optional[str]
    
    # Response
    final_response: str
    response_latency_ms: int
    source_citation: str
    
    # Total
    total_latency_ms: int
    
    # Feedback (collected later)
    user_feedback: Optional[str] = None
    resolved: Optional[bool] = None
