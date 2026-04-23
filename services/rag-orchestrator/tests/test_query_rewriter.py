import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.query_rewriter import rewrite_query


class FakeLLM:
    def generate(self, *args, **kwargs):
        return "câu hỏi đã được viết lại"


def test_self_contained_message_is_not_rewritten():
    message = "Điều trị tăng huyết áp ở người cao tuổi"
    history = [{"role": "user", "content": "Tôi muốn hỏi về tim mạch"}]

    assert rewrite_query(message, history, llm_client=None) == message


def test_rule_based_rewrite_handles_follow_up_without_llm():
    history = [
        {"role": "user", "content": "Hiệu quả gây tê ngoài màng cứng trong chuyển dạ là gì?"},
        {"role": "assistant", "content": "Bài báo cho thấy hiệu quả giảm đau tốt."},
    ]

    rewritten = rewrite_query("what about side effects", history, llm_client=None)

    assert rewritten.startswith("Regarding Hiệu quả gây tê ngoài màng cứng")
    assert "what about side effects" in rewritten


def test_explicit_llm_client_is_used_for_follow_up_rewrite():
    history = [{"role": "user", "content": "Đột quỵ thiếu máu não cấp"}]

    rewritten = rewrite_query("còn trẻ em thì sao", history, llm_client=FakeLLM())

    assert rewritten == "câu hỏi đã được viết lại"
