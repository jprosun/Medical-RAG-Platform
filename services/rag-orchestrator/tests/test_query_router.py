import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.query_router import route_query


def test_routes_explicit_comparison_query_to_comparative_synthesis():
    routed = route_query(
        "Theo context, quyết định chọn CEA hay CAS và việc theo dõi sau can thiệp đều phải cá thể hóa dựa trên những nhóm yếu tố nào?"
    )
    assert routed.query_type == "comparative_synthesis"
    assert routed.retrieval_mode == "article_centric"


def test_routes_single_article_superiority_query_to_comparative_synthesis():
    routed = route_query(
        "Từ phần tổng quan này, có thể kết luận PIEB vượt trội tuyệt đối so với CEI ở mọi khía cạnh không? Có thể khẳng định chắc điều gì và chưa thể khẳng định điều gì?"
    )
    assert routed.query_type == "comparative_synthesis"
    assert routed.retrieval_mode == "article_centric"
    assert routed.answer_style == "bounded_partial"


def test_routes_exact_fact_question_to_exact_answer_style():
    routed = route_query(
        "Trong nghiên cứu này, dấu hiệu lâm sàng phổ biến nhất ở trẻ sơ sinh là gì?"
    )
    assert routed.answer_style == "exact"
    assert routed.requires_numbers is False


def test_routes_list_like_fact_question_to_summary_answer_style():
    routed = route_query(
        "Ở người cao tuổi bị thoái hóa khớp, những biện pháp can thiệp sớm nào được khuyến cáo để làm chậm tiến triển bệnh và giảm triệu chứng?"
    )
    assert routed.answer_style == "summary"
    assert routed.retrieval_mode == "article_centric"
