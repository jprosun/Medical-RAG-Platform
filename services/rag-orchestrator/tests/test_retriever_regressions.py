import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from types import SimpleNamespace

from app.retriever import RetrievedChunk, _build_primary_article_scroll_filter, _same_article_chunk_bonus


def test_same_article_chunk_bonus_prefers_transplant_drug_interaction_evidence():
    query = "Ở bệnh nhân ghép thận nghi mắc lao, vì sao việc chẩn đoán và điều trị phải được cân nhắc cùng nhau thay vì tách rời?"
    generic_text = (
        "Chẩn đoán lao ở bệnh nhân ghép thận gặp nhiều khó khăn do biểu hiện lâm sàng không điển hình "
        "và giới hạn của các phương pháp chẩn đoán hiện tại."
    )
    interaction_text = (
        "Rifampicin làm giảm nồng độ tacrolimus và cyclosporine, làm tăng nguy cơ thải ghép "
        "nên cần theo dõi và chỉnh liều chặt chẽ."
    )

    generic_bonus = _same_article_chunk_bonus(query, generic_text, {"section_title": "Tóm tắt"})
    interaction_bonus = _same_article_chunk_bonus(query, interaction_text, {"section_title": "Tóm tắt"})

    assert interaction_bonus > generic_bonus


def test_same_article_chunk_bonus_penalizes_dense_tables_for_group_factor_queries():
    query = "Theo context, quyết định chọn CEA hay CAS và việc theo dõi sau can thiệp đều phải cá thể hóa dựa trên những nhóm yếu tố nào?"
    factor_text = (
        "Theo dõi sau mổ: bệnh nhân được hẹn tái khám, kiểm tra dấu hiệu tổn thương thần kinh, "
        "siêu âm duplex động mạch cảnh và điều trị nội khoa hỗ trợ để kiểm soát huyết áp, đái tháo đường, rối loạn lipid máu."
    )
    table_text = (
        "Tử vong Đột quỵ TV/ĐQ NMCT RCT 8 0,9 2,0 0,51 1,60 95% CI OR TV/ĐQ tàn phế "
        "CAS CEA 3/3.413 4/4.754 8,7 7,8 5,2 1,60"
    )

    factor_bonus = _same_article_chunk_bonus(query, factor_text, {"section_title": "Tóm tắt"})
    table_bonus = _same_article_chunk_bonus(query, table_text, {"section_title": "Tóm tắt"})

    assert factor_bonus > table_bonus


def test_same_article_chunk_bonus_prefers_methods_section_for_study_design_queries():
    query = "Nghiên cứu này sử dụng thiết kế nào và cỡ mẫu ra sao?"
    methods_text = (
        "Đối tượng và phương pháp nghiên cứu: Nghiên cứu mô tả cắt ngang trên 75 bệnh nhân. "
        "Phương pháp chọn mẫu thuận tiện."
    )
    results_text = (
        "Kết quả: tỷ lệ điều trị thành công là 94,7% và tác dụng phụ chiếm 16%."
    )

    methods_bonus = _same_article_chunk_bonus(query, methods_text, {"section_title": "Đối tượng và phương pháp nghiên cứu"})
    results_bonus = _same_article_chunk_bonus(query, results_text, {"section_title": "Kết quả"})

    assert methods_bonus > results_bonus


def test_primary_article_scroll_filter_expands_legacy_issue_url_title_across_split_doc_ids():
    article = SimpleNamespace(
        title="Ở BỆNH NHÂN LOÉT DẠ DÀY TÁ TRÀNG TẠI BỆNH VIỆN TRƯỜNG ĐẠI HỌC Y KHOA VINH",
        chunks=[
            RetrievedChunk(
                id="legacy-summary",
                text="Summary chunk",
                score=0.7,
                metadata={
                    "title": "Ở BỆNH NHÂN LOÉT DẠ DÀY TÁ TRÀNG TẠI BỆNH VIỆN TRƯỜNG ĐẠI HỌC Y KHOA VINH",
                    "section_title": "SUMMARY",
                    "doc_id": "doc-summary",
                    "source_name": "Tạp chí Y học Việt Nam",
                    "source_url": "https://tapchiyhocvietnam.vn/index.php/vmj/issue/view/311",
                },
            )
        ],
    )

    qfilter = _build_primary_article_scroll_filter(article)

    assert qfilter is not None
    assert qfilter.should is not None
    assert len(qfilter.should) == 2
    title_branch = next(branch for branch in qfilter.should if getattr(branch, "must", None))
    title_keys = [cond.key for cond in title_branch.must]

    assert "title" in title_keys
    assert "source_name" in title_keys
