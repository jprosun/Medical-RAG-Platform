from __future__ import annotations

import importlib


def test_rotate_text_to_title_moves_title_near_front():
    module = importlib.import_module("tools.extract_digital_pdf")
    tail = (
        "TẠP CHÍ Y HỌC VIỆT NAM\nKẾT LUẬN\nMột đoạn cuối bài.\nTÀI LIỆU THAM KHẢO\n"
        "1. A.\n2. B.\n3. C.\n4. D.\n5. E.\n6. F.\n7. G.\n8. H.\n9. I.\n10. J.\n"
        "11. K.\n12. L.\n13. M.\n14. N.\n15. O."
    )
    head = (
        "ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT THAY KHỚP GỐI TOÀN PHẦN "
        "TẠI BỆNH VIỆN ĐẠI HỌC Y HÀ NỘI\n\n"
        "TÓM TẮT\nĐặt vấn đề: Nội dung mở đầu của bài báo."
    )
    text = f"{tail}\n\n{head}"

    reordered = module._rotate_text_to_title(
        text,
        "ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT THAY KHỚP GỐI TOÀN PHẦN TẠI BỆNH VIỆN ĐẠI HỌC Y HÀ NỘI",
        min_prefix_chars=10,
    )

    assert reordered.startswith("ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT THAY KHỚP GỐI TOÀN PHẦN")
    assert "TÓM TẮT" in reordered[:200]
    assert "TÀI LIỆU THAM KHẢO" in reordered
    assert reordered.index("TÀI LIỆU THAM KHẢO") > reordered.index("TÓM TẮT")


def test_rotate_text_to_title_keeps_text_when_title_already_near_front():
    module = importlib.import_module("tools.extract_digital_pdf")
    text = (
        "ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT THAY KHỚP GỐI TOÀN PHẦN "
        "TẠI BỆNH VIỆN ĐẠI HỌC Y HÀ NỘI\n\n"
        "TÓM TẮT\nĐặt vấn đề: Nội dung mở đầu của bài báo.\n\n"
        "KẾT LUẬN\nMột đoạn cuối bài."
    )

    reordered = module._rotate_text_to_title(text, "ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT THAY KHỚP GỐI TOÀN PHẦN TẠI BỆNH VIỆN ĐẠI HỌC Y HÀ NỘI")

    assert reordered == text
