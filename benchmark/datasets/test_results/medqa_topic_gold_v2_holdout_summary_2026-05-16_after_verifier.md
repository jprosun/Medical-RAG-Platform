# Topic Gold v2 Evaluation Summary

- Generated at: 2026-05-16 00:55:39
- Base URL: http://127.0.0.1:8000/api/chat
- Intent: realistic topic/professional medical questions, not article-title guessing.

## Metrics

| Split | Count | HTTP 200 | HTTP 200 Rate | Open Enriched Rate | Article-Centric Rate | Avg Topic Source Hit | Avg Must-Have Hit | Avg Words | Under Min Length | Avg Latency ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| holdout | 4 | 4 | 1.0 | 1.0 | 0.0 | 0.45 | 0.0 | 1167.5 | 0.0 | 64347.725 |
| full_gold | 4 | 4 | 1.0 | 1.0 | 0.0 | 0.45 | 0.0 | 1167.5 | 0.0 | 64347.725 |

## Non-200 Cases

- None

## Low Topic Source Hit (< 0.4)

- holdout holdout_topic_v2_021 hit=0.2 titles=KẾT QUẢ PHÁT HIỆN MÁU ẨN TRONG PHÂN Ở NGƯỜI TỪ 40 TUỔI TRỞ LÊN BẰNG XÉT NGHIỆM HOÁ MIỄN DỊCH TẠI 5 TỈNH MIỀN BẮC VIỆT NAM NĂM 2 | ĐÁNH GIÁ HIỂU BIẾT VÀ SỰ HÀI LÒNG CỦA NGƯỜI BỆNH VỀ VIỆC CUNG CẤP THÔNG TIN CẦN THIẾT TRƯỚC MỔ Ở NGƯỜI BỆNH PHẪU THUẬT TÁI TẠO DÂY CHẰNG CHÉO TRƯỚC TẠI BỆNH VIỆN ĐẠI HỌC Y HÀ NỘI | KẾT QUẢ CA GHÉP HAI PHỔI ĐẦU TIÊN TỪ NGƯỜI HIẾN CHẾT NÃO TẠI BỆNH VIỆN CHỢ RẪY CHO BỆNH NHÂN XƠ PHỔI VÔ CĂN | 60. LIỆU PHÁP TÁI THÍCH ỨNG XÃ HỘI | KẾT QUẢ PHẪU THUẬT ĐIỀU TRỊ UNG THƯ BIỂU MÔ TRỰC TRÀNG TẠI BỆNH VIỆN ĐA KHOA TỈNH NAM ĐỊNH GIAI ĐOẠN 2018-2022 | SO SÁNH ĐẶC ĐIỂM DỊCH TỄ VIÊM NÃO TỰ MIỄN DO KHÁNG THỂ KHÁNG THỤ THỂ N-METHYL-D-ASPARTATE VÀ CÁC VIÊM NÃO KHÁC TẠI TRUNG TÂM THẦN KINH BỆNH VIỆN BẠCH MAI NĂM 2023 | ĐÁNH GIÁ KẾT QUẢ CỦA KÍCH THÍCH ĐIỆN CHỨC NĂNG (FES) PHỐI HỢP HOẠT ĐỘNG TRỊ LIỆU TRONG PHỤC HỒI CHỨC NĂNG CẦM NẮM Ở NGƯỜI BỆNH ĐỘT QUỴ NÃO | KỲ VỌNG CỦA NGƯỜI HỌC VỚI CÁC HÌNH THỨC GIẢNG DẠY, LƯỢNG GIÁ MÔN KÝ SINH TRÙNG TẠI TRƯỜNG ĐẠI HỌC Y TẾ CÔNG CỘNG TRONG GIAI ĐOẠN 2020 - 2

## Under Minimum Length

- None

## Unexpected Article-Centric Routing

- None
