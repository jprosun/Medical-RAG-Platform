from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipelines.etl.vn import vn_title_extractor


def test_dav_title_extractor_rescues_appendix_form_title():
    body = """
Phụ lục 1
MẪU BÁO CÁO
Kết quả lựa chọn nhà thầu cung cấp thuốc
(Ban hành kèm theo Thông tư số 15/2019/TT-BYT)
Kính gửi:
"""
    title = vn_title_extractor.extract(
        "dav_gov",
        body,
        yaml_title="",
        file_url="https://dav.gov.vn/images/upload_file/2019/phu-luc-1_1563382581.pdf",
    )

    assert "Kết quả lựa chọn nhà thầu cung cấp thuốc" in title
    assert title.lower() != "phụ lục 1"
