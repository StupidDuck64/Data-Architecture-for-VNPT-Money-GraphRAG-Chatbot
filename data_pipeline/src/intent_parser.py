import json
import logging
from typing import List, Optional

from schema import (
    StructuredQueryObject,
    ServiceEnum,
    ProblemTypeEnum,
    Message,
    Config,
)

logger = logging.getLogger(__name__)


class IntentParserHybrid:
    def __init__(self, llm_client):
        self.llm_client = llm_client
        self.rule_parser = IntentParserLocal()
        self.llm_parser = IntentParserLLM(llm_client)
        self.llm_threshold = 0.6 
    
    def parse(
        self,
        user_message: str,
        chat_history: Optional[List[Message]] = None
    ) -> StructuredQueryObject:
    
        rule_result = self.rule_parser.parse(user_message, chat_history)
        
        # If confident enough, use rule-based
        if rule_result.confidence_intent >= self.llm_threshold:
            logger.info(f"Using rule-based result (conf={rule_result.confidence_intent:.2f})")
            return rule_result
        
        # Otherwise use LLM
        logger.info(f"Rule-based low confidence ({rule_result.confidence_intent:.2f}), using LLM")
        return self.llm_parser.parse(user_message, chat_history)


class IntentParser(IntentParserHybrid):
    
    pass


class IntentParserLLM:
    """
    Parse user message into StructuredQueryObject using LLM.
    
    Key principles:
    1. LLM chỉ làm slot filling, KHÔNG sinh answer
    2. Temperature = 0 for deterministic parsing
    3. Output is validated JSON, không free-form
    """
    
    SYSTEM_PROMPT = """Bạn là một parser chuyên trích xuất thông tin có cấu trúc từ tin nhắn người dùng cho dịch vụ hỗ trợ VNPT Money.

NHIỆM VỤ DUY NHẤT của bạn là trích xuất thông tin và trả về JSON. KHÔNG được tạo câu trả lời hay giải thích.

Trả về JSON với các trường sau:

{
    "service": "<string> - xem danh sách services bên dưới",
    "problem_type": "<string> - một trong: khong_nhan_otp, that_bai, pending_lau, vuot_han_muc, tru_tien_chua_nhan, loi_ket_noi, huong_dan, chinh_sach, khac",
    "topic": "<string|null> - chủ đề cụ thể nếu xác định được",
    "bank": "<string|null> - tên ngân hàng nếu được đề cập",
    "amount": "<number|null> - số tiền nếu được đề cập",
    "error_code": "<string|null> - mã lỗi nếu được đề cập",
    "need_account_lookup": "<boolean> - true nếu người dùng yêu cầu kiểm tra thông tin giao dịch CÁ NHÂN của họ",
    "is_out_of_domain": "<boolean> - true CHỈ KHI câu hỏi hoàn toàn không liên quan đến VNPT Money",
    "confidence_intent": "<float 0-1> - độ tự tin trong việc trích xuất",
    "missing_slots": "<array> - danh sách các trường cần làm rõ",
    "condensed_query": "<string> - câu hỏi đã được chuẩn hóa cho tìm kiếm (QUAN TRỌNG: xem quy tắc bên dưới)"
}

=== QUY TẮC TẠO condensed_query (RẤT QUAN TRỌNG) ===

Đây là trường quan trọng nhất để tìm kiếm semantic. Phải tuân thủ:

1. **Trích xuất VẤN ĐỀ CỐT LÕI** từ câu hỏi phức tạp:
   - Loại bỏ thông tin cá nhân (số tiền cụ thể, tên ngân hàng cụ thể nếu không cần thiết)
   - Giữ lại BẢN CHẤT vấn đề
   - Dùng từ ngữ CHUẨN của hệ thống

2. **Mapping các biến thể về dạng chuẩn:**
   - "chuyển từ [ngân hàng] sang VNPT Money" → "nạp tiền từ ngân hàng vào ví"
   - "nạp từ [ngân hàng]" → "nạp tiền từ ngân hàng vào ví"
   - "tiền không vào/chưa cộng/chưa nhận được" → "ví không cộng tiền" hoặc "chưa nhận được tiền"
   - "bị trừ nhưng không cộng" → "ngân hàng trừ tiền nhưng ví không cộng"
   - "chuyển tiền bị lỗi" → "chuyển tiền thất bại"
   - "rút về ngân hàng chưa nhận" → "rút tiền về ngân hàng chưa nhận được"
   - "giao dịch đang chờ/pending" → "giao dịch đang chờ xử lý"

3. **Ví dụ mapping thực tế:**
   - Input: "tôi mới chuyển từ mb sang tài khoản vnpt money 21 củ nhưng vnpt money của tôi chưa cộng tiền"
   - Phân tích: Người dùng chuyển tiền từ MB Bank vào VNPT Money, ngân hàng đã trừ nhưng ví chưa cộng
   - condensed_query: "Nạp tiền từ ngân hàng vào ví VNPT Money bị trừ tiền nhưng ví không cộng"

   - Input: "ck từ vcb qua vnpt 5tr mà chờ cả ngày chưa thấy"
   - Phân tích: Chuyển tiền từ Vietcombank, chưa nhận được, chờ lâu
   - condensed_query: "Nạp tiền từ ngân hàng vào ví chờ lâu chưa nhận được tiền"

   - Input: "nạp tiền mà trừ rồi ko thấy vào"
   - condensed_query: "Nạp tiền bị trừ tiền ngân hàng nhưng ví không cộng"

4. **Đặc biệt với các lỗi giao dịch:**
   - Nếu có dấu hiệu "trừ tiền + không nhận" → nhấn mạnh cả hai
   - Nếu có "chờ lâu/pending" → thêm "đang chờ xử lý"
   - Nếu có "lỗi/thất bại" → thêm "giao dịch lỗi/thất bại"

=== DANH SÁCH SERVICES (chọn 1 trong các giá trị sau) ===

DỊCH VỤ TÀI CHÍNH CƠ BẢN:
- nap_tien: nạp tiền điện thoại, nạp nhầm số
- rut_tien: rút tiền Mobile Money, ví điện tử
- chuyen_tien: chuyển tiền Mobile Money, chuyển nhầm
- lien_ket_ngan_hang: liên kết/hủy liên kết ngân hàng, thẻ ATM
- thanh_toan: thanh toán dịch vụ, thanh toán tự động

TÀI KHOẢN & BẢO MẬT:
- otp: mã OTP, SmartOTP, không nhận OTP
- han_muc: hạn mức giao dịch, vượt hạn mức
- dang_ky: đăng ký tài khoản VNPT Money, Mobile Money
- dinh_danh: định danh eKYC, xác minh CCCD
- bao_mat: mật khẩu, bảo mật, khóa tài khoản

DỊCH VỤ VIỄN THÔNG (QUAN TRỌNG):
- data_3g_4g: gói data, gói cước, Data 3G/4G, gia hạn gói data
- mua_the: mua mã thẻ, thẻ nạp, thẻ cào
- di_dong_tra_sau: cước di động trả sau
- hoa_don_vien_thong: hóa đơn viễn thông, internet VNPT/Viettel

TIỀN ĐIỆN NƯỚC:
- tien_dien: thanh toán tiền điện, hóa đơn điện
- tien_nuoc: thanh toán tiền nước
- dien_nuoc_khac: điện nước khác, phí chung cư, vệ sinh môi trường

TÀI CHÍNH - BẢO HIỂM:
- bao_hiem: mua bảo hiểm, bồi thường, tra cứu hợp đồng
- vay: thanh toán vay tiêu dùng, FE Credit, MSB Credit
- tiet_kiem: tiết kiệm online

GIÁO DỤC:
- hoc_phi: đóng học phí VnEdu, học phí đại học

VÉ & ĐẶT CHỖ:
- mua_ve: vé tàu, vé máy bay, đặt phòng khách sạn, vé xe

DỊCH VỤ CÔNG:
- dich_vu_cong: nộp phạt giao thông, thuế, BHXH/BHYT

GIẢI TRÍ:
- giai_tri: MyTV, VTVcab, Vietlott, vòng quay may mắn

KHÁC:
- ung_dung: tải app, cập nhật ứng dụng VNPT Money
- dieu_khoan: điều khoản sử dụng, biểu phí, chính sách hợp đồng, quy định Mobile Money, Ví doanh nghiệp
- quyen_rieng_tu: quyền riêng tư, xử lý thông tin khách hàng, ủy quyền bên thứ ba, lưu trữ/chia sẻ/thu thập thông tin, quyền và nghĩa vụ khách hàng
- khac: không xác định được service cụ thể

=== QUY TẮC QUAN TRỌNG ===

1. VNPT MONEY HỖ TRỢ TẤT CẢ các dịch vụ trên → is_out_of_domain = false cho tất cả câu hỏi về:
   - Gói data/cước/viễn thông → service = "data_3g_4g"
   - Hóa đơn điện/nước/internet → service phù hợp
   - Bảo hiểm, vay, tiết kiệm → service phù hợp
   - Vé tàu/máy bay/khách sạn → service = "mua_ve"
   - Mobile Money, ví điện tử → service phù hợp
   - Xử lý thông tin, ủy quyền, bên thứ ba, quyền riêng tư → service = "quyen_rieng_tu"
   - Điều khoản, hợp đồng, quy định dịch vụ → service = "dieu_khoan"
   - LIÊN KẾT ngân hàng với VNPT Money (MB, VCB, BIDV...) → service = "lien_ket_ngan_hang"

2. need_account_lookup = true CHỈ KHI:
   - "Kiểm tra giao dịch của tôi"
   - "Tiền tôi chuyển đã đến chưa"
   - "Xem số dư tài khoản"

3. is_out_of_domain = true KHI:
   - Thời tiết, chứng khoán, thể thao, chính trị
   - Sản phẩm/dịch vụ không liên quan VNPT
   - **QUAN TRỌNG**: Hỏi về dịch vụ RIÊNG của ngân hàng/ví khác (KHÔNG liên quan VNPT Money):
     + "hình thức thanh toán của MB/VCB/Momo" → out of domain (hỏi về dịch vụ của MB Bank)
     + "cách chuyển tiền trên app MB/VCB" → out of domain (hỏi về app ngân hàng khác)
     + "phí dịch vụ của Momo/ZaloPay" → out of domain (hỏi về ví điện tử khác)
   - **NHƯNG** các câu hỏi sau KHÔNG phải out of domain:
     + "liên kết MB với VNPT Money" → service = "lien_ket_ngan_hang"
     + "chuyển tiền từ MB về VNPT Money" → service = "chuyen_tien"
     + "thanh toán qua VNPT Money bằng thẻ MB" → service = "thanh_toan"

4. Luôn cung cấp condensed_query dù confidence thấp

=== VÍ DỤ VỀ LỖI GIAO DỊCH (QUAN TRỌNG) ===

Input: "tôi mới chuyển từ mb sang tài khoản vnpt money 21 củ nhưng vnpt money của tôi chưa cộng tiền"
Phân tích: Người dùng chuyển tiền từ MB Bank vào VNPT Money, đã bị trừ tiền ngân hàng nhưng ví chưa cộng
Output:
{
    "service": "nap_tien",
    "problem_type": "tru_tien_chua_nhan",
    "topic": "nap_tien_tu_ngan_hang_loi",
    "bank": "MB Bank",
    "amount": 21000000,
    "error_code": null,
    "need_account_lookup": true,
    "is_out_of_domain": false,
    "confidence_intent": 0.95,
    "missing_slots": [],
    "condensed_query": "Nạp tiền từ ngân hàng vào ví VNPT Money bị trừ tiền nhưng ví không cộng"
}

Input: "ck từ vcb qua vnpt 5tr mà chờ cả ngày chưa thấy"
Phân tích: Chuyển khoản từ Vietcombank vào VNPT Money, chờ lâu chưa nhận
Output:
{
    "service": "nap_tien",
    "problem_type": "tru_tien_chua_nhan",
    "topic": "nap_tien_tu_ngan_hang_loi",
    "bank": "Vietcombank",
    "amount": 5000000,
    "error_code": null,
    "need_account_lookup": true,
    "is_out_of_domain": false,
    "confidence_intent": 0.93,
    "missing_slots": [],
    "condensed_query": "Nạp tiền từ ngân hàng vào ví VNPT Money bị trừ tiền nhưng ví không cộng chờ lâu"
}

Input: "nạp tiền từ ngân hàng mà trừ rồi ko thấy vào ví"
Output:
{
    "service": "nap_tien",
    "problem_type": "tru_tien_chua_nhan",
    "topic": "nap_tien_tu_ngan_hang_loi",
    "bank": null,
    "amount": null,
    "error_code": null,
    "need_account_lookup": true,
    "is_out_of_domain": false,
    "confidence_intent": 0.95,
    "missing_slots": [],
    "condensed_query": "Nạp tiền từ ngân hàng vào ví VNPT Money bị trừ tiền nhưng ví không cộng"
}

Input: "chuyển tiền cho bạn nhưng báo thất bại, tiền có mất không"
Output:
{
    "service": "chuyen_tien",
    "problem_type": "that_bai",
    "topic": "chuyen_tien_that_bai",
    "bank": null,
    "amount": null,
    "error_code": null,
    "need_account_lookup": false,
    "is_out_of_domain": false,
    "confidence_intent": 0.95,
    "missing_slots": [],
    "condensed_query": "Chuyển tiền thất bại tiền có bị mất không"
}

Input: "rút tiền từ ví về ngân hàng mà chờ nửa ngày chưa thấy"
Output:
{
    "service": "rut_tien",
    "problem_type": "tru_tien_chua_nhan",
    "topic": "rut_tien_chua_nhan",
    "bank": null,
    "amount": null,
    "error_code": null,
    "need_account_lookup": true,
    "is_out_of_domain": false,
    "confidence_intent": 0.94,
    "missing_slots": [],
    "condensed_query": "Rút tiền từ ví về ngân hàng chưa nhận được tiền"
}

=== VÍ DỤ KHÁC ===

Input: "Gói data có tự động gia hạn không?"
Output:
{
    "service": "data_3g_4g",
    "problem_type": "huong_dan",
    "topic": "gia_han_goi_data",
    "bank": null,
    "amount": null,
    "error_code": null,
    "need_account_lookup": false,
    "is_out_of_domain": false,
    "confidence_intent": 0.95,
    "missing_slots": [],
    "condensed_query": "Gói data có tự động gia hạn không"
}

Input: "Mua data cho người khác được không?"
Output:
{
    "service": "data_3g_4g",
    "problem_type": "huong_dan",
    "topic": "mua_data_cho_nguoi_khac",
    "bank": null,
    "amount": null,
    "error_code": null,
    "need_account_lookup": false,
    "is_out_of_domain": false,
    "confidence_intent": 0.93,
    "missing_slots": [],
    "condensed_query": "Mua data cho người khác được không"
}

Input: "Thanh toán tiền điện như thế nào?"
Output:
{
    "service": "tien_dien",
    "problem_type": "huong_dan",
    "topic": "thanh_toan_tien_dien",
    "bank": null,
    "amount": null,
    "error_code": null,
    "need_account_lookup": false,
    "is_out_of_domain": false,
    "confidence_intent": 0.94,
    "missing_slots": [],
    "condensed_query": "Hướng dẫn thanh toán tiền điện"
}

Input: "Quy trình bồi thường bảo hiểm"
Output:
{
    "service": "bao_hiem",
    "problem_type": "huong_dan",
    "topic": "boi_thuong_bao_hiem",
    "bank": null,
    "amount": null,
    "error_code": null,
    "need_account_lookup": false,
    "is_out_of_domain": false,
    "confidence_intent": 0.92,
    "missing_slots": [],
    "condensed_query": "Quy trình bồi thường bảo hiểm"
}

Input: "Ủy quyền hoặc thuê bên thứ ba xử lý thông tin"
Output:
{
    "service": "quyen_rieng_tu",
    "problem_type": "huong_dan",
    "topic": "uy_quyen_xu_ly_thong_tin",
    "bank": null,
    "amount": null,
    "error_code": null,
    "need_account_lookup": false,
    "is_out_of_domain": false,
    "confidence_intent": 0.95,
    "missing_slots": [],
    "condensed_query": "Ủy quyền bên thứ ba xử lý thông tin khách hàng"
}

Input: "VNPT xử lý thông tin khách hàng như thế nào?"
Output:
{
    "service": "quyen_rieng_tu",
    "problem_type": "huong_dan",
    "topic": "xu_ly_thong_tin_khach_hang",
    "bank": null,
    "amount": null,
    "error_code": null,
    "need_account_lookup": false,
    "is_out_of_domain": false,
    "confidence_intent": 0.94,
    "missing_slots": [],
    "condensed_query": "VNPT xử lý thông tin khách hàng như thế nào"
}

Input: "Hôm nay thời tiết thế nào?"
Output:
{
    "service": "khac",
    "problem_type": "khac",
    "topic": null,
    "bank": null,
    "amount": null,
    "error_code": null,
    "need_account_lookup": false,
    "is_out_of_domain": true,
    "confidence_intent": 0.98,
    "missing_slots": [],
    "condensed_query": "Hỏi về thời tiết - ngoài phạm vi"
}

Input: "tôi muốn hỏi kĩ hơn về hình thức thanh toán của mb"
Output:
{
    "service": "khac",
    "problem_type": "khac",
    "topic": null,
    "bank": "MB Bank",
    "amount": null,
    "error_code": null,
    "need_account_lookup": false,
    "is_out_of_domain": true,
    "confidence_intent": 0.95,
    "missing_slots": [],
    "condensed_query": "Hỏi về dịch vụ thanh toán của MB Bank - ngoài phạm vi VNPT Money"
}

Input: "cách chuyển tiền trên app Momo"
Output:
{
    "service": "khac",
    "problem_type": "khac",
    "topic": null,
    "bank": null,
    "amount": null,
    "error_code": null,
    "need_account_lookup": false,
    "is_out_of_domain": true,
    "confidence_intent": 0.95,
    "missing_slots": [],
    "condensed_query": "Hỏi về cách chuyển tiền trên Momo - ngoài phạm vi VNPT Money"
}

Input: "liên kết ngân hàng MB với VNPT Money"
Output:
{
    "service": "lien_ket_ngan_hang",
    "problem_type": "huong_dan",
    "topic": "lien_ket_mb",
    "bank": "MB Bank",
    "amount": null,
    "error_code": null,
    "need_account_lookup": false,
    "is_out_of_domain": false,
    "confidence_intent": 0.95,
    "missing_slots": [],
    "condensed_query": "Hướng dẫn liên kết ngân hàng MB với VNPT Money"
}"""

    def __init__(self, llm_client):
        self.llm_client = llm_client
        self.model = Config.INTENT_PARSER_MODEL
        self.temperature = Config.INTENT_PARSER_TEMPERATURE
        self.max_tokens = Config.INTENT_PARSER_MAX_TOKENS
    
    def parse(
        self, 
        user_message: str, 
        chat_history: Optional[List[Message]] = None
    ) -> StructuredQueryObject:
        """
        chuyển đổi user message thành StructuredQueryObject.
        
        Args:
            user_message: Tin nhắn hiện tại của người dùng
            chat_history: Các tin nhắn trước đó để làm ngữ cảnh
            
        Returns:
            StructuredQueryObject with extracted slots
        """
        # Build context from history
        history_context = self._build_history_context(chat_history or [])
        
        # Build user prompt
        user_prompt = self._build_user_prompt(user_message, history_context)
        
        try:
            # Call LLM
            response = self.llm_client.chat.completions.create(
                model=self.model,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"}
            )
            
            # Parse response
            result_json = json.loads(response.choices[0].message.content)
            
            # Validate and convert to StructuredQueryObject
            return self._convert_to_structured_query(result_json, user_message)
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            return self._create_fallback_query(user_message)
            
        except Exception as e:
            logger.error(f"Intent parsing failed: {e}")
            return self._create_fallback_query(user_message)
    
    def _build_history_context(self, chat_history: List[Message]) -> str:
        """Build context string from chat history."""
        if not chat_history:
            return ""
        
        history_lines = []
        for msg in chat_history[-Config.CHAT_HISTORY_MAX_MESSAGES:]:
            role = "Người dùng" if msg.role == "user" else "Bot"
            history_lines.append(f"{role}: {msg.content}")
        
        return "\n".join(history_lines)
    
    def _build_user_prompt(self, user_message: str, history_context: str) -> str:
        """Build the user prompt for LLM."""
        if history_context:
            return f"""LỊCH SỬ CHAT:
{history_context}

TIN NHẮN HIỆN TẠI:
{user_message}

Phân tích tin nhắn hiện tại với ngữ cảnh từ lịch sử chat. Trả về JSON."""
        else:
            return f"""TIN NHẮN:
{user_message}

Trả về JSON."""
    
    def _convert_to_structured_query(
        self, 
        result: dict, 
        original_message: str
    ) -> StructuredQueryObject:
        """Convert LLM JSON response to StructuredQueryObject."""
        
        # Parse service enum
        service_str = result.get("service", "khac")
        try:
            service = ServiceEnum(service_str)
        except ValueError:
            service = ServiceEnum.KHAC
        
        # Parse problem_type enum
        problem_type_str = result.get("problem_type", "khac")
        try:
            problem_type = ProblemTypeEnum(problem_type_str)
        except ValueError:
            problem_type = ProblemTypeEnum.KHAC
        
        return StructuredQueryObject(
            service=service,
            problem_type=problem_type,
            condensed_query=result.get("condensed_query", original_message),
            topic=result.get("topic"),
            bank=result.get("bank"),
            amount=result.get("amount"),
            error_code=result.get("error_code"),
            need_account_lookup=result.get("need_account_lookup", False),
            is_out_of_domain=result.get("is_out_of_domain", False),
            confidence_intent=result.get("confidence_intent", 0.5),
            missing_slots=result.get("missing_slots", []),
            original_message=original_message
        )
    
    def _create_fallback_query(self, user_message: str) -> StructuredQueryObject:
        return StructuredQueryObject(
            service=ServiceEnum.KHAC,
            problem_type=ProblemTypeEnum.KHAC,
            condensed_query=user_message,
            need_account_lookup=False,
            is_out_of_domain=False,
            confidence_intent=0.3,
            missing_slots=["service", "problem_type"],
            original_message=user_message
        )


# ==============================================================================
# TEXT NORMALIZER - Handle không dấu, viết tắt, teencode
# ==============================================================================

class TextNormalizer:
    """
    Normalize Vietnamese text: không dấu → có dấu, viết tắt → đầy đủ.
    """
    
    # Viết tắt phổ biến
    ABBREVIATIONS = {
        # Đại từ
        "t": "tôi", "m": "mình", "mk": "mình", "mik": "mình",
        "b": "bạn", "bn": "bạn", "ban": "bạn",
        "a": "anh", "e": "em", "c": "chị", "chi": "chị",
        
        # Phủ định/khẳng định
        "k": "không", "ko": "không", "khong": "không", "kg": "không", "kh": "không",
        "dc": "được", "đc": "được", "duoc": "được", "dk": "được",
        "r": "rồi", "roi": "rồi",
        "cx": "cũng",  # Removed "cung" - breaks "nhà cung cấp"
        "vs": "với", "voi": "với",  # Removed "v" - too short and ambiguous
        "j": "gì",  # Removed "gi", "z" - breaks "digilife", etc.
        "d": "vậy",  # Removed "v", "vay" - breaks "vay tiêu dùng"
        "ns": "nói",  # Removed "noi" - breaks "noi dia" -> "nội địa"
        "lm": "làm",  # Removed "lam" - keep in NO_ACCENT_MAP only
        "sao": "sao", "ntn": "như thế nào", "nhu the nao": "như thế nào",
        
        # Hành động phổ biến
        "lk": "liên kết", "lien ket": "liên kết",
        "ck": "chuyển khoản", "chuyen khoan": "chuyển khoản",
        "ct": "chuyển tiền", "chuyen tien": "chuyển tiền",
        "nt": "nạp tiền", "nap tien": "nạp tiền",
        "rt": "rút tiền", "rut tien": "rút tiền",
        "tk": "tài khoản", "tai khoan": "tài khoản",
        "nh": "ngân hàng", "ngan hang": "ngân hàng",
        "tt": "thanh toán", "thanh toan": "thanh toán",
        "gd": "giao dịch", "giao dich": "giao dịch",
        "dk": "đăng ký", "dang ky": "đăng ký",
        "dn": "đăng nhập", "dang nhap": "đăng nhập",
        "mk": "mật khẩu", "mat khau": "mật khẩu",
        "sdt": "số điện thoại", "so dien thoai": "số điện thoại",
        "otp": "OTP",
        
        # Trạng thái
        "tb": "thất bại", "that bai": "thất bại",
        "tc": "thành công", "thanh cong": "thành công",
        "bl": "bị lỗi", "bi loi": "bị lỗi",
        "loi": "lỗi",
        "ht": "hỗ trợ", "ho tro": "hỗ trợ",
        "hd": "hướng dẫn", "huong dan": "hướng dẫn",
        
        # Câu hỏi
        "sn": "sao nhỉ", "lj": "làm gì",
        
        # Ngân hàng
        "vcb": "vietcombank", "vietcombank": "Vietcombank",
        "tcb": "techcombank", "techcombank": "Techcombank",
        "mb": "MB Bank", "mbbank": "MB Bank",
        "bidv": "BIDV",
        "vp": "VPBank", "vpbank": "VPBank",
        "acb": "ACB",
        "tpb": "TPBank", "tpbank": "TPBank",
        "scb": "SCB",
        "shb": "SHB",
        "msb": "MSB",
        
        # Từ không dấu phổ biến - REMOVED short words that cause false positives
        # "vi": "vì",  # breaks "dịch vụ" etc.
        # "la": "là",  # breaks "digilife", "lazada" etc.
        # "ma": "mà",  # breaks many words
        # "da": "đã",  # breaks "lazada", "sendo" etc.
        "chua": "chưa", "se": "sẽ", "dang": "đang",
        # "bao": "báo",  # breaks "thuê bao", "bảo hiểm" etc.
        "can": "cần", "muon": "muốn",
        "the": "thẻ", "tien": "tiền", "phi": "phí",
        "dien": "điện", "nuoc": "nước",
        "cach": "cách", "huong": "hướng",
    }
    
    # Không dấu → Có dấu (các từ quan trọng)
    NO_ACCENT_MAP = {
        # Dịch vụ
        "lien ket": "liên kết",
        "ngan hang": "ngân hàng",
        "chuyen tien": "chuyển tiền",
        "chuyen khoan": "chuyển khoản",
        "nap tien": "nạp tiền",
        "rut tien": "rút tiền",
        "thanh toan": "thanh toán",
        "giao dich": "giao dịch",
        "tai khoan": "tài khoản",
        "dang ky": "đăng ký",
        "dang nhap": "đăng nhập",
        "mat khau": "mật khẩu",
        "so dien thoai": "số điện thoại",
        
        # Trạng thái
        "that bai": "thất bại",
        "thanh cong": "thành công",
        "bi loi": "bị lỗi",
        "ho tro": "hỗ trợ",
        "huong dan": "hướng dẫn",
        "khong duoc": "không được",
        
        # Các từ đơn - REMOVED short words that cause false positives
        "khong": "không",
        "duoc": "được",
        # "chua": "chưa",  # Keep - usually safe
        "roi": "rồi",
        # "gi": "gì",  # REMOVED - breaks "digilife", "login", etc.
        # "vay": "vậy",  # REMOVED - breaks "khoản vay", "vay tiêu dùng"
        "lam": "làm",
        "nhu the nao": "như thế nào",
        "the nao": "thế nào",
        "vi sao": "vì sao",
        "tai sao": "tại sao",
        " gi ": " gì ",  # Only standalone "gi" with spaces
        " gi?": " gì?",  # Common ending pattern
        
        # Dịch vụ cụ thể
        "tien dien": "tiền điện",
        "tien nuoc": "tiền nước",
        "hoa don": "hóa đơn",
        "bao hiem": "bảo hiểm",
        "hoc phi": "học phí",
        "vi dien tu": "ví điện tử",
        "mobile money": "Mobile Money",
        
        # Ngân hàng
        "ngan hang": "ngân hàng",
        "the tin dung": "thẻ tín dụng",
        "the ghi no": "thẻ ghi nợ",
        
        # Lỗi/vấn đề
        "co loi": "có lỗi",
        "khong ho tro": "không hỗ trợ",
        "bi tu choi": "bị từ chối",
        "qua han": "quá hạn",
        "het han": "hết hạn",
        
        # Thẻ ATM & liên kết
        "noi dia": "nội địa",
        "the atm": "thẻ ATM",
        "the atm noi dia": "thẻ ATM nội địa",
        "lien ket ngan hang": "liên kết ngân hàng",
        "khong lien ket duoc": "không liên kết được",
        
        # Chuyển tiền & giao dịch
        "chua thay": "chưa thấy",
        "tru tien": "trừ tiền",
        "chua nhan": "chưa nhận",
        "da nhan": "đã nhận",
        "chuyen sang": "chuyển sang",
        "chua tru": "chưa trừ",
        "da tru": "đã trừ",
        "ben nay": "bên này",
        "ben kia": "bên kia",
        
        # Trạng thái phổ biến
        "dang xu ly": "đang xử lý",
        "da xu ly": "đã xử lý",
        "chua xu ly": "chưa xử lý",
        "bi loi": "bị lỗi",
        "khong thanh cong": "không thành công",
        "that bai": "thất bại",
        "dang cho": "đang chờ",
        
        # Từ đơn phổ biến (cẩn thận với false positive)
        "moi": "mới",
        "nhung": "nhưng",
        "cung": "cũng",
        "nua": "nữa",
        "dau": "đâu",
        "sao lai": "sao lại",
        "the nay": "thế này",
        "nhu vay": "như vậy",
        
        # Hỏi đáp
        "lam sao": "làm sao",
        "nhu nao": "như nào",
        "o dau": "ở đâu",
        "khi nao": "khi nào",
        "bao lau": "bao lâu",
        "bao nhieu": "bao nhiêu",
        
        # Nạp/rút tiền
        "nap tien": "nạp tiền",
        "rut tien": "rút tiền",
        "chua nap": "chưa nạp",
        "da nap": "đã nạp",
        "nap nham": "nạp nhầm",
        "rut nham": "rút nhầm",
        "chuyen nham": "chuyển nhầm",
        
        # OTP & bảo mật
        "khong nhan duoc": "không nhận được",
        "chua nhan duoc": "chưa nhận được",
        "gui lai": "gửi lại",
        "ma otp": "mã OTP",
        "xac nhan": "xác nhận",
        "xac thuc": "xác thực",
        
        # Tài khoản
        "tai khoan cua toi": "tài khoản của tôi",
        "so du": "số dư",
        "kiem tra": "kiểm tra",
        "cap nhat": "cập nhật",
        "thay doi": "thay đổi",
        "dang nhap": "đăng nhập",
        "dang xuat": "đăng xuất",
        
        # Thanh toán
        "thanh toan": "thanh toán",
        "chua thanh toan": "chưa thanh toán",
        "da thanh toan": "đã thanh toán",
        "phi dich vu": "phí dịch vụ",
        "phi giao dich": "phí giao dịch",
        
        # Hỗ trợ
        "ho tro": "hỗ trợ",
        "lien he": "liên hệ",
        "tong dai": "tổng đài",
        "hotline": "hotline",
        
        # Từ đơn phổ biến (cẩn thận với substring)
        "toi": "tôi",
        " de ": " để ",
        " vao ": " vào ",
        " cho ": " cho ",
        " tu ": " từ ",
        " den ": " đến ",
        " cua ": " của ",
        " cu ": " cứ ",
        " la ": " là ",
        " co ": " có ",
        " bi ": " bị ",
        " di ": " đi ",
        " ve ": " về ",
        " ra ": " ra ",
        
        # Cụm từ quan trọng
        "de nap": "để nạp",
        "de rut": "để rút",
        "de chuyen": "để chuyển",
        "de thanh toan": "để thanh toán",
        "de dang ky": "để đăng ký",
        "vao tai khoan": "vào tài khoản",
        "tu tai khoan": "từ tài khoản",
        "cua toi": "của tôi",
        "cu bao": "cứ báo",
        "cu hien": "cứ hiện",
        
        # Kiểm tra & xem
        "kiem tra so du": "kiểm tra số dư",
        "xem so du": "xem số dư",
        "xem lich su": "xem lịch sử",
        "lich su giao dich": "lịch sử giao dịch",
        
        # Học phí
        "hoc phi": "học phí",
        "thu phi": "thu phí",
        "dong hoc phi": "đóng học phí",
        "nop hoc phi": "nộp học phí",
        "phi hoc": "phí học",
        "phi dai hoc": "phí đại học",
        
        # Dịch vụ công
        "nop phat": "nộp phạt",
        "phat giao thong": "phạt giao thông",
        "nop phat giao thong": "nộp phạt giao thông",
        "nop thue": "nộp thuế",
        "le phi": "lệ phí",
        "truoc ba": "trước bạ",
        "dich vu cong": "dịch vụ công",
        
        # Mua vé
        "mua ve": "mua vé",
        "ve tau": "vé tàu",
        "ve may bay": "vé máy bay",
        "ve xe": "vé xe",
        "dat ve": "đặt vé",
        "huy ve": "hủy vé",
        "hoan ve": "hoàn vé",
        "dat phong": "đặt phòng",
        " tau": " tàu",
        
        # Bảo hiểm
        "bao hiem": "bảo hiểm",
        "phi bao hiem": "phí bảo hiểm",
        "hop dong bao hiem": "hợp đồng bảo hiểm",
        
        # Di động
        "di dong": "di động",
        "tra sau": "trả sau",
        "goi cuoc": "gói cước",
        
        # Học phí (thêm)
        " hoc": " học",
        
        # Phương tiện
        "phuong tien": "phương tiện",
        "phuong tien di chuyen": "phương tiện di chuyển",
        "di chuyen": "di chuyển",
        
        # Nhà cung cấp
        "nha cung cap": "nhà cung cấp",
        "cung cap nuoc": "cung cấp nước",
        "danh sach": "danh sách",
    }
    
    @classmethod
    def normalize(cls, text: str) -> str:
        """
        Normalize text: viết tắt → đầy đủ, không dấu → có dấu.
        
        Args:
            text: Raw user input
            
        Returns:
            Normalized text
        """
        if not text:
            return text
            
        # Lowercase for matching
        text_lower = text.lower().strip()
        
        # Step 1: Replace multi-word phrases first (longer matches first)
        for no_accent, with_accent in sorted(cls.NO_ACCENT_MAP.items(), key=lambda x: -len(x[0])):
            text_lower = text_lower.replace(no_accent, with_accent)
        
        # Step 2: Replace single-word abbreviations
        words = text_lower.split()
        normalized_words = []
        for word in words:
            # Check if word is abbreviation
            if word in cls.ABBREVIATIONS:
                normalized_words.append(cls.ABBREVIATIONS[word])
            else:
                normalized_words.append(word)
        
        return " ".join(normalized_words)


class IntentParserLocal:
    """
    Rule-based intent parser for testing without LLM.
    Uses keyword matching as fallback with PRIORITY ordering.
    
    More specific services are checked BEFORE general ones.
    """
    
    # ========================================================================
    # COMPREHENSIVE KEYWORDS - Ordered by PRIORITY (more specific FIRST)
    # ========================================================================
    # CRITICAL: Order matters! More specific services checked before general ones
    # 
    # Coverage target: 100% of all 458 problems in database
    # ========================================================================
    
    SERVICE_KEYWORDS_PRIORITY = [
        # ============================================================
        # GROUP 1: ĐIỀU KHOẢN (130 problems)
        # ============================================================
        # DIEU_KHOAN phải check TRƯỚC QUYEN_RIENG_TU vì có overlap về "dữ liệu", "thông tin"
        (ServiceEnum.DIEU_KHOAN, [
            # Core terms
            "điều khoản", "điều kiện sử dụng", "điều khoản dịch vụ", "điều khoản chấp thuận",
            # Ví điện tử VNPT Pay - MUST CHECK FIRST
            "ví điện tử vnpt pay", "ví điện tử vnpt", "vnpt pay",
            "mở ví điện tử", "sử dụng ví điện tử", "đóng ví", "tạm khóa ví", "phong tỏa ví",
            "trích nợ tự động", "biểu phí ví", "phí dịch vụ ví",
            "quyền sở hữu trí tuệ", "hoàn trả tiền vào ví", "miễn trách nhiệm",
            "bất khả kháng", "hiệu lực thỏa thuận", "chuyển nhượng quyền",
            # Mobile Money - specific terms
            "mobile money", "dịch vụ mobile money", "tài khoản mobile money",
            "mở tài khoản mobile", "đóng tài khoản mobile", "phong tỏa tài khoản mobile",
            "biểu phí mobile money", "hạn mức mobile money",
            "giao dịch không hủy ngang", "tra soát mobile", "khiếu nại mobile",
            # Ví doanh nghiệp - specific terms
            "ví doanh nghiệp", "ví doanh nghiệp vnpt", "vnpt pay doanh nghiệp",
            "tổ chức sử dụng ví", "ủy quyền sử dụng ví", "doanh nghiệp vnpt pay",
            "khách hàng doanh nghiệp", "thông tin doanh nghiệp",
            # SmartOTP điều khoản - MUST be before OTP service
            "smartotp điều khoản", "kích hoạt smartotp", "phạm vi smartotp",
            "quyền smartotp", "nghĩa vụ smartotp", "phạm vi áp dụng smartotp",
            "sử dụng smartotp", "điều kiện smartotp", "cung cấp dịch vụ smartotp",
            "quyền của khách hàng khi sử dụng smartotp", "quyền khi sử dụng smartotp",
            "nghĩa vụ khi sử dụng smartotp", "trách nhiệm khi sử dụng smartotp",
            "quyền của vnpt smartotp", "nghĩa vụ vnpt smartotp",
            "quy định smartotp", "hạn mức giao dịch smartotp",
            "phương thức xác thực smartotp", "xác thực smartotp",
            # CRITICAL: SmartOTP nghĩa vụ VNPT - must match before OTP
            "nghĩa vụ của vnpt đối với khách hàng khi cung cấp dịch vụ smartotp",
            "nghĩa vụ của vnpt khi cung cấp dịch vụ smartotp",
            "nghĩa vụ vnpt đối với khách hàng smartotp", "dịch vụ smartotp",
            # Thanh toán hóa đơn tự động
            "thanh toán hóa đơn tự động", "trích nợ hóa đơn",
            "điều kiện điều khoản thanh toán", "sửa đổi bổ sung điều khoản",
            # Giao dịch quốc tế - COMPREHENSIVE KEYWORDS
            "giao dịch quốc tế", "thanh toán quốc tế", "tắt bật dịch vụ", "hủy dịch vụ quốc tế",
            "dịch vụ giao dịch quốc tế", "phạm vi giao dịch quốc tế",
            "đối tượng giao dịch quốc tế", "quy trình tắt bật", "tắt/bật",
            "thu thập xử lý dữ liệu giao dịch quốc tế", "nghĩa vụ cung cấp thông tin",
            "quản lý nguồn tiền giao dịch quốc tế", "trách nhiệm thanh toán giao dịch quốc tế",
            "từ chối xử lý giao dịch quốc tế", "hạn mức biểu phí giao dịch quốc tế",
            "biểu phí dịch vụ giao dịch quốc tế", "hạn mức dịch vụ giao dịch quốc tế",
            "sai sót giao dịch quốc tế", "phát hiện sai sót giao dịch",
            # SPECIFIC TITLES for giao dịch quốc tế
            "dịch vụ giao dịch quốc tế áp dụng cho những đối tượng",
            "nghĩa vụ cung cấp thông tin và phạm vi vnpt thu thập",
            "quy định về quản lý nguồn tiền và trách nhiệm thanh toán",
            "các trường hợp vnpt được quyền từ chối xử lý giao dịch",
            "quy định về hạn mức và biểu phí dịch vụ giao dịch",
            "quy trình và nghĩa vụ của khách hàng khi phát hiện sai sót",
            # Liên kết thanh toán điều khoản
            "thỏa thuận liên kết", "hủy liên kết thanh toán", "đối tác liên kết",
            # Hành vi vi phạm
            "hành vi bị cấm", "hành vi nghiêm cấm",
            # Tra soát và khiếu nại - MUST be in DIEU_KHOAN
            "tra soát", "khiếu nại", "kênh tiếp nhận yêu cầu tra soát",
            "thời hạn xử lý tra soát", "kết quả tra soát",
            "đối soát", "hạch toán sai", "xử lý hạch toán",
            # Quyền và nghĩa vụ trong DIEU_KHOAN context
            "quyền của vnpt", "quyền vnpt cung cấp", "trách nhiệm nghĩa vụ vnpt",
            "quyền nghĩa vụ khi sử dụng", "quyền nghĩa vụ cung cấp dịch vụ",
            "trách nhiệm nghĩa vụ vnpt", "trách nhiệm nghĩa vụ khi cung cấp",
            "nghĩa vụ trách nhiệm của vnpt", "nghĩa vụ trách nhiệm của khách hàng",
            # Specific phrases
            "luật áp dụng tranh chấp", "giải quyết tranh chấp",
            "bồi hoàn tổn thất", "sai phạm pháp luật giao dịch",
            "phạm vi quyền truy cập dữ liệu", "trách nhiệm bảo mật ví",
            "miễn trừ trách nhiệm", "sự kiện bất khả kháng",
            "hiệu lực áp dụng", "thay đổi bản điều khoản",
            # Chữ ký điện tử trong DIEU_KHOAN
            "chữ ký điện tử", "chữ ký điện tử khi sử dụng",
            # Cơ chế xác thực và liên kết
            "cơ chế xác thực", "xác thực sau khi liên kết",
            "cơ sở pháp lý", "quy trình giải quyết mâu thuẫn",
            # Hoàn trả tiền
            "hoàn trả tiền", "các trường hợp được hoàn trả",
            "giải quyết khiếu nại sản phẩm",
            # Giới hạn trách nhiệm - VNPT-Media
            "giới hạn trách nhiệm của vnpt-media", "giới hạn trách nhiệm vnpt-media",
            "trách nhiệm của vnpt-media đối với hàng hóa", "quy trình sử dụng",
            # Trách nhiệm và nghĩa vụ specific
            "trách nhiệm và nghĩa vụ của vnpt", "trách nhiệm của khách hàng",
            "trách nhiệm cung cấp thông tin", "trách nhiệm xác thực dữ liệu",
            "trách nhiệm bảo mật tài khoản", "trách nhiệm miễn trừ",
            "giới hạn trách nhiệm", "giới hạn trách nhiệm của vnpt",
            # CRITICAL: Specific phrases for DIEU_KHOAN "thông tin" context
            # These override QUYEN_RIENG_TU when in ví điện tử/mobile money context
            "mục đích sử dụng thông tin và dữ liệu cá nhân của khách hàng",
            "trong những trường hợp nào vnpt được cung cấp thông tin khách hàng",
            "phạm vi khách hàng đồng ý cho phép vnpt sử dụng thông tin",
            # General policy
            "policy", "terms", "quy định dịch vụ", "sửa đổi điều khoản",
        ]),
        
        # ============================================================
        # GROUP 2: QUYỀN RIÊNG TƯ (33 problems) - Check after DIEU_KHOAN
        # ============================================================
        (ServiceEnum.QUYEN_RIENG_TU, [
            # Core privacy terms - UNIQUE to QUYEN_RIENG_TU
            "quyền riêng tư", "chính sách riêng tư", "bảo vệ thông tin cá nhân",
            "xử lý thông tin khách hàng", "thu thập thông tin cá nhân",
            "ủy quyền xử lý", "bên thứ ba xử lý",
            "lưu trữ thông tin khách hàng", "chia sẻ thông tin khách hàng",
            "mục đích xử lý thông tin", "quyền của khách hàng đối với thông tin",
            # Specific QUYEN_RIENG_TU terms from problem titles
            "thông tin bắt buộc cung cấp", "thông tin tùy chọn cung cấp",
            "thông tin bắt buộc", "thông tin tùy chọn",
            "hệ quả không cung cấp thông tin", "hệ quả khi không cung cấp",
            "loại dữ liệu cá nhân", "loại dữ liệu", "dữ liệu cá nhân được xử lý",
            "mục đích bắt buộc cung cấp", "mục đích bắt buộc", "mục đích bắt buộc để",
            "mục đích thực hiện nghĩa vụ", "mục đích để thực hiện nghĩa vụ pháp luật",
            "mục đích tùy chọn", "từ chối tiếp thị", "cách thức từ chối",
            "mua bán thông tin khách hàng", "chính sách về việc mua bán",
            "vnpt làm gì với thông tin", "vnpt làm gì",
            "đối tượng được chia sẻ thông tin", "đối tượng vnpt được phép chia sẻ",
            "phạm vi sử dụng thông tin",
            "quyền được biết", "quyền đồng ý", "quyền truy cập thông tin",
            "truy cập và chỉnh sửa", "chỉnh sửa thông tin cá nhân",
            "quyền phản đối xử lý", "quyền phản đối", "hạn chế xử lý",
            "rút lại đồng ý", "rút lại sự đồng ý",
            "xóa dữ liệu cá nhân", "xóa dữ liệu",
            "khiếu nại tố cáo", "yêu cầu bồi thường", "quyền khiếu nại",
            "tự bảo vệ pháp lý", "tự bảo vệ",
            # Nghĩa vụ của khách hàng (QUYEN_RIENG_TU context)
            "nghĩa vụ của khách hàng bảo vệ", "tổng hợp các nghĩa vụ của khách hàng",
            "tổng hợp các nghĩa vụ", "nghĩa vụ tự bảo vệ tài khoản",
            "nghĩa vụ cung cấp thông tin", "chi tiết nghĩa vụ tự bảo vệ",
            "tôn trọng thông tin người khác", "nghĩa vụ tôn trọng",
            # Lưu trữ thông tin
            "tiêu chuẩn bảo mật lưu trữ", "địa điểm và tiêu chuẩn bảo mật",
            "thời gian lưu trữ thông tin", "thời gian lưu trữ",
            # Nghĩa vụ của VNPT (QUYEN_RIENG_TU context)
            "nghĩa vụ cam kết vnpt", "tổng hợp các nghĩa vụ và cam kết bảo mật của vnpt",
            "cam kết bảo mật của vnpt",
            "biện pháp bảo vệ thông tin", "các biện pháp bảo vệ thông tin",
            "ngăn chặn truy cập trái phép",
            "sự cố hệ thống bảo mật", "trách nhiệm xử lý yêu cầu",
            # Rủi ro và thiệt hại
            "rủi ro thiệt hại", "các rủi ro và thiệt hại không mong muốn",
            "thiệt hại không mong muốn",
            "sự cố kỹ thuật bồi thường", "trách nhiệm bồi thường khi xảy ra sự cố",
            "trách nhiệm bồi thường",
            # Quảng cáo và bên thứ ba
            "quảng cáo internet bên thứ ba", "quảng cáo của bên thứ ba",
            "quy định về quảng cáo",
            "liên kết bên thứ ba", "phạm vi điều chỉnh đối với liên kết",
            "liên kết của bên thứ ba",
            # Luật và liên hệ
            "luật áp dụng chính sách", "quy định về luật áp dụng cho chính sách",
            "luật áp dụng cho chính sách",
            "phương thức liên hệ giải đáp", "các phương thức liên hệ để giải đáp",
            "phương thức liên hệ", "liên hệ để giải đáp",
            # Trách nhiệm VNPT xử lý
            "trách nhiệm vnpt xử lý", "trách nhiệm và các trường hợp vnpt xử lý",
            "trách nhiệm và các trường hợp", "vnpt xử lý thông tin",
            # General terms - less specific
            "thông tin cá nhân", "dữ liệu cá nhân", "bảo vệ thông tin",
            "thông tin khách hàng", "ủy quyền", "tiếp thị quảng cáo",
            "nghĩa vụ khách hàng", "cam kết bảo mật vnpt",
        ]),
        
        # ============================================================
        # GROUP 3: DỊCH VỤ CỤ THỂ (264 problems) - Check by specificity
        # ============================================================
        
        # --- 3.1 Nạp tiền điện thoại ---
        (ServiceEnum.NAP_TIEN, [
            "nạp tiền điện thoại", "nạp điện thoại", "nạp tiền", "top up", "topup",
            "nạp nhầm", "nhà mạng", "thuê bao", "nạp tiền tự động",
            "thuê bao chưa nhận", "tài khoản bị trừ tiền nhưng thuê bao",
            # Lỗi nạp tiền
            "trừ tiền nhưng thuê bao chưa nhận", "bị trừ tiền nhưng thuê bao",
            "tại sao tài khoản bị trừ tiền nhưng thuê bao",
            # SPECIFIC TITLE
            "tại sao tài khoản bị trừ tiền nhưng thuê bao chưa nhận được",
        ]),
        
        # --- 3.2 Tiền điện ---
        (ServiceEnum.TIEN_DIEN, [
            "tiền điện", "hóa đơn điện", "mã pe", "mã khách hàng điện",
            "thanh toán điện", "gạch nợ điện",
        ]),
        
        # --- 3.3 Tiền nước ---
        (ServiceEnum.TIEN_NUOC, [
            "tiền nước", "hóa đơn nước", "nhà cung cấp nước", "gạch nợ nước",
            "danh sách nhà cung cấp nước", "nhà cung cấp nước chưa đầy đủ",
            "lưu mẫu hóa đơn tiền nước", "thời gian gạch nợ hóa đơn nước",
            # SPECIFIC TITLE
            "danh sách nhà cung cấp nước chưa đầy đủ?",
        ]),
        
        # --- 3.4 Điện nước khác ---
        (ServiceEnum.DIEN_NUOC_KHAC, [
            "phí chung cư", "vệ sinh môi trường", "điện nước khác",
        ]),
        
        # --- 3.5 Bảo hiểm ---
        (ServiceEnum.BAO_HIEM, [
            "bảo hiểm", "bồi thường bảo hiểm", "hợp đồng bảo hiểm",
            "manulife", "vietinbank bảo hiểm", "bảo hiểm số", "phí bảo hiểm",
            "tra cứu hợp đồng bảo hiểm", "thanh toán phí bảo hiểm", "bảo hiểm định kỳ",
        ]),
        
        # --- 3.6 Vay - MUST BE BEFORE THANH_TOAN (priority matching)
        (ServiceEnum.VAY, [
            "vay", "khoản vay", "vay tiêu dùng", "gạch nợ vay", "hợp đồng vay",
            "fe credit", "msb credit", "aeon finance", "mirae asset",
            "nhập sai số hợp đồng vay",
            # CRITICAL: Specific vay keywords - must match BEFORE thanh_toan
            "thanh toán khoản vay", "thanh toán vay tiêu dùng", "thanh toán vay",
            "hướng dẫn thanh toán khoản vay", "thanh toán vay bao lâu",
            "phí thanh toán vay", "phí thanh toán vay tiêu dùng",
            "nhập sai số hợp đồng vay có lấy lại tiền",
            # Additional vay terms to ensure priority
            "khoản vay tiêu dùng", "vay bao lâu", "được gạch nợ",
            "số hợp đồng vay", "hợp đồng vay có lấy lại",
            # SPECIFIC TITLES - EXACT MATCH
            "hướng dẫn thanh toán khoản vay tiêu dùng",
            "thanh toán vay bao lâu thì được gạch nợ",
            "nhập sai số hợp đồng vay có lấy lại tiền được không",
            "phí thanh toán vay tiêu dùng",
        ]),
        
        # --- 3.7 Tiết kiệm ---
        (ServiceEnum.TIET_KIEM, [
            "tiết kiệm", "tiết kiệm online", "gửi tiết kiệm", "siêu tích lũy",
        ]),
        
        # --- 3.8 Học phí ---
        (ServiceEnum.HOC_PHI, [
            "học phí", "đóng học phí", "vnedu", "phí đại học", "học phí ssc",
            "học sinh", "biên lai học phí", "thu phí đh", "thu phí cđ",
            "nộp phí xét tuyển", "phí xét tuyển đại học", "học mãi", "dtsoft", "asc",
            # Thêm keywords
            "phí học", "thu phí học", "nộp học phí",
        ]),
        
        # --- 3.9 Mua vé (cần bao gồm phương tiện di chuyển) ---
        (ServiceEnum.MUA_VE, [
            "vé tàu", "vé máy bay", "đặt phòng", "khách sạn", "vé xe", "olala",
            "vé sự kiện", "vé vui chơi", "đặt vé", "hủy vé", "hoàn vé",
            "nhận vé", "thuế phí khách sạn", "nhận phòng", "trả phòng",
            "thu phí bến xe", "đặt xe taxi",
            # Phương tiện di chuyển - specific keywords
            "phương tiện di chuyển", "di chuyển",
            # VAT và hóa đơn vé
            "xuất hóa đơn vat cho vé", "xuất hóa đơn đỏ", "hóa đơn vé",
            # Giá phòng
            "giá phòng", "giá phòng trên app", "thuế phí chưa", "giá bao gồm thuế",
            # Thay đổi ngày đặt
            "thay đổi ngày đặt phòng", "thay đổi ngày đặt",
        ]),
        
        # --- 3.10 Dịch vụ công ---
        (ServiceEnum.DICH_VU_CONG, [
            "nộp phạt", "phạt giao thông", "thuế", "lệ phí trước bạ",
            "bhxh", "bhyt", "bảo hiểm xã hội", "bảo hiểm y tế",
            "cổng dịch vụ công", "dịch vụ công",
        ]),
        
        # --- 3.11 Giải trí ---
        (ServiceEnum.GIAI_TRI, [
            "mytv", "vtvcab", "truyền hình", "k+", "truyền hình k",
            "vietlott", "vòng quay", "may mắn", "mua vé vietlott", "nhận thưởng",
            "dealtoday", "du hí",
            # DigiLife - COMPREHENSIVE KEYWORDS - MUST MATCH BEFORE THANH_TOAN
            "digilife", "gói cước digilife", "dịch vụ digilife",
            "hướng dẫn sử dụng digilife", "lịch sử giao dịch digilife",
            "lỗi thanh toán digilife", "digilife bị treo",
            "digilife hỗ trợ nguồn tiền", "nguồn tiền digilife",
            "gói cước digilife bị treo", "giao dịch digilife",
            # SPECIFIC TITLES for DigiLife - ALL 4 PROBLEMS
            "hướng dẫn sử dụng dịch vụ gói cước digilife",
            "kiểm tra lịch sử giao dịch gói cước digilife",
            "lỗi thanh toán gói cước digilife bị treo",
            "dịch vụ gói cước digilife hỗ trợ nguồn tiền nào",
            # Vietlott specific
            "mua vé hộ người khác", "mua vé hộ", "vé hộ người khác",
            "thời gian ngừng bán vé", "ngừng bán vé", "ngừng bán vé trong ngày",
        ]),
        
        # --- 3.12 Data 3G/4G ---
        (ServiceEnum.DATA_3G_4G, [
            "data", "3g", "4g", "gói cước data", "gói data", "mua data",
            "gia hạn data", "gói cước 3g", "gói cước 4g",
            "mua gói data", "gói data báo lỗi", "tại sao mua gói data báo lỗi",
        ]),
        
        # --- 3.13 Mua mã thẻ ---
        (ServiceEnum.MUA_THE, [
            "mua thẻ", "mã thẻ", "thẻ cào", "thẻ nạp", "mua mã thẻ",
        ]),
        
        # --- 3.14 Di động trả sau ---
        (ServiceEnum.DI_DONG_TRA_SAU, [
            "trả sau", "cước trả sau", "di động trả sau", "cước di động",
        ]),
        
        # --- 3.15 Hóa đơn viễn thông ---
        (ServiceEnum.HOA_DON_VIEN_THONG, [
            "hóa đơn viễn thông", "cước internet", "internet vnpt", "internet viettel",
            "viễn thông vntt", "điện thoại cố định", "cước viễn thông",
            "hóa đơn điện tử", "thanh toán cước",
            "lấy hóa đơn điện tử", "hóa đơn điện tử sau khi thanh toán",
        ]),
        
        # --- 3.16 Thanh toán chung ---
        (ServiceEnum.THANH_TOAN, [
            "thanh toán", "trả tiền", "payment", "pay",
            "đơn hàng vnpt", "pvoil", "xăng dầu",
            "lazada", "sendo", "điểm thanh toán",
            "thanh toán tự động", "thanh toán trả trước",
            # Specific services
            "vtvcab", "internet vnpt", "đơn hàng vnpt",
        ]),
        
        # --- 3.17 Chuyển tiền ---
        (ServiceEnum.CHUYEN_TIEN, [
            "chuyển tiền", "transfer", "chuyển nhầm",
            "nhận tiền kiều hối", "kiều hối", "tiền kiều hối",
        ]),
        
        # ============================================================
        # GROUP 4: TÀI KHOẢN & BẢO MẬT
        # ============================================================
        (ServiceEnum.OTP, [
            # NOTE: "smartotp" removed to let SmartOTP-related DIEU_KHOAN match first
            "otp", "mã xác nhận", "mã xác thực", "không nhận otp",
        ]),
        (ServiceEnum.HAN_MUC, [
            "hạn mức", "limit", "giới hạn giao dịch", "vượt hạn mức", "nâng hạn mức",
        ]),
        (ServiceEnum.DANG_KY, [
            "đăng ký", "register", "tạo tài khoản", "mở tài khoản", "đăng ký mobile money",
        ]),
        (ServiceEnum.DINH_DANH, [
            "định danh", "xác minh", "verify", "ekyc", "cccd", "định danh tự động", "định danh thủ công",
        ]),
        (ServiceEnum.BAO_MAT, [
            # NOTE: "chữ ký điện tử" removed to let DIEU_KHOAN match first for terms context
            "bảo mật", "security", "mật khẩu", "password", "khóa tài khoản", "tạm khóa",
        ]),
        
        # ============================================================
        # GROUP 5: DỊCH VỤ CHUNG (check LAST)
        # ============================================================
        (ServiceEnum.RUT_TIEN, [
            "rút tiền", "withdraw", "rút tiền về ngân hàng",
        ]),
        (ServiceEnum.LIEN_KET_NGAN_HANG, [
            # NOTE: "liên kết thanh toán" removed to let DIEU_KHOAN match first for terms context
            "liên kết ngân hàng", "liên kết bank", "thẻ atm", "hủy liên kết",
            "liên kết tài khoản",
        ]),
        (ServiceEnum.UNG_DUNG, [
            "ứng dụng", "app", "tải ứng dụng", "cập nhật ứng dụng", "download", "phiên bản",
            "vnpt money app", "tải vnpt money",
            # Các tính năng của ứng dụng
            "kiểm tra số dư", "xem số dư", "số dư tài khoản", "số dư",
            "lịch sử giao dịch", "xem lịch sử",
        ]),
    ]
    
    # Keep old dict for backward compatibility
    SERVICE_KEYWORDS = {svc: kws for svc, kws in SERVICE_KEYWORDS_PRIORITY}
    
    PROBLEM_KEYWORDS = {
        ProblemTypeEnum.KHONG_NHAN_OTP: ["không nhận", "chưa nhận otp", "không có otp"],
        ProblemTypeEnum.THAT_BAI: ["thất bại", "fail", "lỗi", "error", "không được"],
        ProblemTypeEnum.PENDING_LAU: ["pending", "chờ lâu", "đang xử lý"],
        ProblemTypeEnum.VUOT_HAN_MUC: ["vượt hạn mức", "quá hạn mức", "exceed limit"],
        ProblemTypeEnum.TRU_TIEN_CHUA_NHAN: ["trừ tiền", "chưa nhận", "mất tiền"],
        ProblemTypeEnum.LOI_KET_NOI: ["kết nối", "connection", "mạng"],
        ProblemTypeEnum.HUONG_DAN: ["hướng dẫn", "làm sao", "cách", "how to"],
        ProblemTypeEnum.CHINH_SACH: ["chính sách", "quy định", "policy"],
    }
    
    ACCOUNT_LOOKUP_KEYWORDS = [
        "kiểm tra giao dịch",
        "xem giao dịch",
        "tiền của tôi",
        "giao dịch của tôi",
        "tra cứu",
        "đã chuyển chưa",
        "đã nhận chưa",
    ]
    
    # Danh sách ngân hàng/dịch vụ bên ngoài VNPT Money
    EXTERNAL_BANKS = [
        "mb bank", "mbbank", "mb", "vietcombank", "vcb", "techcombank", "tcb",
        "bidv", "vietinbank", "ctg", "vpbank", "vp bank", "acb", "tpbank",
        "sacombank", "scb", "shb", "msb", "hdbank", "ocb", "seabank",
        "eximbank", "nam a bank", "bac a bank", "pvcombank", "vib",
        "agribank", "lienvietpostbank", "lpb", "ncb", "abbank", "baoviet bank",
        "momo", "zalopay", "shopee pay", "shopeepay", "airpay", "grabpay",
        "vietnam airlines", "vietjet", "bamboo airways",
    ]
    
    # Cụm từ gợi ý hỏi về dịch vụ RIÊNG của ngân hàng khác (không liên quan VNPT Money)
    EXTERNAL_SERVICE_PATTERNS = [
        "của mb", "của vietcombank", "của vcb", "của techcombank", "của tcb",
        "của bidv", "của vietinbank", "của vpbank", "của acb", "của tpbank",
        "của sacombank", "của momo", "của zalopay", "của shopee",
        "bên mb", "bên vietcombank", "bên vcb", "bên techcombank", "bên bidv",
        "bên momo", "bên zalopay", "bên shopee",
        "trên mb", "trên vietcombank", "trên vcb", "trên momo", "trên zalopay",
        "app mb", "app vcb", "app vietcombank", "app techcombank", "app bidv",
        "app momo", "app zalopay", "ứng dụng mb", "ứng dụng vcb",
        "dịch vụ của mb", "dịch vụ của vcb", "dịch vụ của momo",
        "hình thức thanh toán của mb", "thanh toán của mb", "thanh toán của vcb",
        "thanh toán của momo", "thanh toán của zalopay",
        "chuyển tiền trên mb", "chuyển tiền trên vcb", "chuyển tiền trên momo",
    ]
    
    # Cụm từ cho thấy câu hỏi LIÊN QUAN đến VNPT Money (không phải out of domain)
    VNPT_RELATED_PATTERNS = [
        "liên kết", "liên kết với", "liên kết ngân hàng", "liên kết mb", "liên kết vcb",
        "qua vnpt", "trên vnpt", "bằng vnpt", "vnpt money",
        "mobile money", "ví điện tử vnpt", "vnpt pay",
        "chuyển tiền từ", "chuyển tiền về", "nạp tiền từ", "rút tiền về",
        "thanh toán qua vnpt", "thanh toán bằng vnpt",
    ]
    
    def _is_out_of_domain(self, message_lower: str) -> bool:
        """
        Phát hiện câu hỏi ngoài phạm vi VNPT Money.
        
        Out of domain nếu:
        1. Hỏi về dịch vụ RIÊNG của ngân hàng/ví khác (không liên quan VNPT Money)
        2. Hỏi về chủ đề không liên quan (thời tiết, chính trị, thể thao...)
        
        KHÔNG out of domain nếu:
        - Hỏi về liên kết ngân hàng với VNPT Money
        - Hỏi về chuyển/nạp/rút tiền qua VNPT Money (dù có nhắc đến ngân hàng khác)
        """
        # Nếu có pattern liên quan VNPT Money -> KHÔNG out of domain
        if any(pattern in message_lower for pattern in self.VNPT_RELATED_PATTERNS):
            return False
        
        # Kiểm tra các pattern hỏi về dịch vụ của ngân hàng/ví khác
        for pattern in self.EXTERNAL_SERVICE_PATTERNS:
            if pattern in message_lower:
                logger.info(f"Out of domain detected: asking about external service '{pattern}'")
                return True
        
        # Kiểm tra nếu chỉ hỏi về ngân hàng mà không có context VNPT Money
        # Ví dụ: "hình thức thanh toán của mb" -> hỏi về MB Bank, không phải VNPT
        for bank in self.EXTERNAL_BANKS:
            # Pattern: "của <bank>" hoặc "bên <bank>" hoặc "trên <bank>" 
            # mà không có "liên kết", "vnpt", "mobile money"
            if f"của {bank}" in message_lower or f"bên {bank}" in message_lower:
                # Đã check VNPT_RELATED_PATTERNS ở trên, nếu đến đây nghĩa là không liên quan VNPT
                logger.info(f"Out of domain detected: asking about '{bank}' without VNPT context")
                return True
        
        return False
    
    def parse(
        self, 
        user_message: str, 
        chat_history: Optional[List[Message]] = None
    ) -> StructuredQueryObject:
        """
        Rule-based parsing using PRIORITY-ordered keyword matching.
        More specific services are matched before general ones.
        
        Includes text normalization for:
        - Không dấu (lien ket → liên kết)
        - Viết tắt (ko → không, tk → tài khoản)
        - Teencode (t → tôi, b → bạn)
        """
        # NORMALIZE TEXT FIRST - handle không dấu, viết tắt, teencode
        normalized_message = TextNormalizer.normalize(user_message)
        message_lower = normalized_message.lower()
        
        logger.debug(f"Original: {user_message}")
        logger.debug(f"Normalized: {normalized_message}")
        
        # CHECK OUT OF DOMAIN FIRST
        is_out_of_domain = self._is_out_of_domain(message_lower)
        
        # Nếu out of domain, trả về ngay với confidence cao
        if is_out_of_domain:
            return StructuredQueryObject(
                service=ServiceEnum.KHAC,
                problem_type=ProblemTypeEnum.KHAC,
                condensed_query=normalized_message,
                need_account_lookup=False,
                is_out_of_domain=True,
                confidence_intent=0.9,  # High confidence that it's out of domain
                missing_slots=[],
                original_message=user_message
            )
        
        # Detect service using PRIORITY order
        service = ServiceEnum.KHAC
        for svc, keywords in self.SERVICE_KEYWORDS_PRIORITY:
            if any(kw in message_lower for kw in keywords):
                service = svc
                break
        
        # Detect problem type
        problem_type = ProblemTypeEnum.KHAC
        for prob, keywords in self.PROBLEM_KEYWORDS.items():
            if any(kw in message_lower for kw in keywords):
                problem_type = prob
                break
        
        # Detect account lookup need
        need_account_lookup = any(
            kw in message_lower for kw in self.ACCOUNT_LOOKUP_KEYWORDS
        )
        
        # Calculate confidence
        confidence = 0.5
        if service != ServiceEnum.KHAC:
            confidence += 0.2
        if problem_type != ProblemTypeEnum.KHAC:
            confidence += 0.2
        
        # Detect missing slots
        missing_slots = []
        if service == ServiceEnum.KHAC:
            missing_slots.append("service")
        if problem_type == ProblemTypeEnum.KHAC and not need_account_lookup:
            missing_slots.append("problem_type")
        
        return StructuredQueryObject(
            service=service,
            problem_type=problem_type,
            condensed_query=normalized_message,  # Use normalized for better retrieval
            need_account_lookup=need_account_lookup,
            is_out_of_domain=False,
            confidence_intent=min(confidence, 1.0),
            missing_slots=missing_slots,
            original_message=user_message  # Keep original for logging
        )
