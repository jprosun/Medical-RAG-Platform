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


def test_self_contained_definition_not_contaminated_by_prior_topic():
    # Regression: a topic-switch definition question must not inherit the
    # previous turn's disease, even when short (<=5 words). Previously this
    # 5-word question was rule-based-rewritten to "Regarding ung thư cổ tử cung...".
    history = [
        {"role": "user", "content": "Ung thư cổ tử cung là gì? Có chữa được không?"},
        {"role": "assistant", "content": "Ung thư cổ tử cung là..."},
    ]
    # No LLM and explicit LLM must both leave it unchanged (rewriting is skipped).
    assert rewrite_query("Ung thư máu là gì?", history, llm_client=None) == "Ung thư máu là gì?"
    assert rewrite_query("Ung thư máu là gì?", history, llm_client=FakeLLM()) == "Ung thư máu là gì?"


def test_self_contained_subject_question_not_rewritten():
    history = [
        {"role": "user", "content": "Ung thư cổ tử cung là gì?"},
        {"role": "assistant", "content": "..."},
    ]
    # Has its own subject ("ung thư máu") -> standalone, not a follow-up fragment.
    assert rewrite_query("Ung thư máu chữa được không?", history, llm_client=FakeLLM()) == "Ung thư máu chữa được không?"


def test_referent_follow_up_still_rewritten():
    history = [{"role": "user", "content": "Hen phế quản là gì?"}]
    assert rewrite_query("what about side effects", history, llm_client=FakeLLM()) == "câu hỏi đã được viết lại"
