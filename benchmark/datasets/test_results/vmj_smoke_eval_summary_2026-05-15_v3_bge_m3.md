# VMJ Smoke Eval Summary - v3 BGE-M3

- Generated at: 2026-05-15 14:11:02
- Base URL: http://127.0.0.1:8000/api/chat
- Raw output: `benchmark/datasets/test_results/vmj_smoke_eval_raw_2026-05-15_v3_bge_m3.jsonl`

## Metrics

| Count | HTTP 200 | HTTP 200 Rate | Degraded | Degraded Rate | Avg Latency (ms) | Top1 Title Hit Rate | Avg Must-Have Hit Rate | Must-Not Violation Rate |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 12 | 12 | 100.0% | 1 | 8.3% | 34692.6 | 0.0 | 0.044 | 0.0 |

## Retrieval Misses

- `q_008` expected `THOÁI HÓA KHỚP, THÁCH THỨC VỚI SỨC KHỎE NGƯỜI CAO TUỔI VÀ “CỬA SỔ CƠ HỘI” CHO ĐIỀU TRỊ`; top1 `NGHIÊN CỨU TÌNH HÌNH, MỘT SỐ YẾU TỐ LIÊN QUAN, ĐÁNH GIÁ HIỆU QUẢ PHƯƠNG PHÁP TẬP VẬN ĐỘNG TRÊN BỆNH NHÂN CAO TUỔI THOÁI HOÁ KHỚP GỐI`
- `q_009` expected `KHẢO SÁT CÁC TRƯỜNG HỢP TRẺ SINH TỪ MẸ NHIỄM LIÊN CẦU KHUẨN NHÓM B TẠI BỆNH VIỆN ĐA KHOA TÂM ANH HỒ CHÍ MINH`; top1 `ĐẶC ĐIỂM LÂM SÀNG, CẬN LÂM SÀNG BỆNH NHÂN SUY TIM CẤP NHẬP VIỆN TẠI BỆNH VIỆN CHỢ RẪY`
- `q_013` expected `ỨNG DỤNG KỸ THUẬT ĐỊNH VỊ THẦN KINH TRONG PHẪU THUẬT U TẾ BÀO HÌNH SAO BẬC THẤP TẠI BỆNH VIỆN HỮU NGHỊ VIỆT ĐỨC`; top1 `ĐÁNH GIÁ KẾT QUẢ SỚM PHẪU THUẬT CHẤN THƯƠNG SỌ NÃO NẶNG TẠI BỆNH VIỆN ĐA KHOA TỈNH BẮC GIANG`
- `q_017` expected `QUẢN LÝ HOẠT ĐỘNG MUA BÁN THUỐC TẠI CÁC NHÀ THUỐC TƯ NHÂN TRÊN ĐỊA BÀN QUẬN PHÚ NHUẬN, THÀNH PHỐ HỒ CHÍ MINH NĂM 2023`; top1 `máu Bệnh viện Quân Y 105, Luận văn thạc sĩ dược học, Đại học Dược Hà Nội, Hà Nội. thuốc tại bệnh viện Đại học Y Dược Thành phố Hồ Chí Minh cơ sở 2 trong tháng 06/2015", bệnh nhân viêm khớp tại Khoa Nội Tim mạch Bệnh viện đa khoa Trung ương Cần Thơ năm`
- `q_035` expected `ĐƯỜNG CONG HỌC TẬP ĐIỀU TRỊ LỖ TIỂU THẤP THỂ GIỮA VÀ THỂ SAU BẰNG KỸ THUẬT SNODGRASS`; top1 `NGHIÊN CỨU GIÁ TRỊ CỦA CHỈ SỐ ROMA - IOTA TRONG DỰ ĐOÁN NGUY CƠ U ÁC BUỒNG TRỨNG TẠI BỆNH VIỆN PHỤ SẢN THÀNH PHỐ CẦN THƠ NĂM 2017 – 2019 Nguyễn Quốc Bảo*, Lưu Thị Thanh Đào`
- `q_038` expected `ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ BỆNH NHÂN SUY TIM MÃN TÍNH ĐIỀU TRỊ TẠI KHOA TIM MẠCH - LÃO HỌC BỆNH VIỆN ĐA KHOA HUYỆN HOÀI ĐỨC`; top1 `ĐÁNH GIÁ HIỆU QUẢ VÀ ĐỘ AN TOÀN CỦA DAPAGLIFLOZIN SO VỚI EMPAGLIFLOZIN TRONG ĐIỀU TRỊ SUY TIM MẠN TÍNH TẠI BỆNH VIỆN HỮU NGHỊ ĐA KHOA NGHỆ AN`
- `q_081` expected `SỰ THAY ĐỔI ĐẶC ĐIỂM LÂM SÀNG TRÊN BỆNH NHÂN TRƯỞNG THÀNH BỊ RỐI LOẠN KHỚP THÁI DƯƠNG HÀM (TMD) ĐƯỢC ĐIỀU TRỊ BẰNG MÁNG ỔN ĐỊNH (SS) 6 THÁNG`; top1 `ĐÁNH GIÁ HIỆU QUẢ SÀNG LỌC BỆNH LÝ RỐI LOẠN NỘI KHỚP THÁI DƯƠNG HÀM BẰNG THIẾT BỊ ĐO ĐỘ RUNG KHỚP`
- `q_091` expected `KHẢO SÁT MỘT SỐ YẾU TỐ LIÊN QUAN ĐẾN BỆNH LÝ ĐÁI THÁO ĐƯỜNG THAI KỲ CỦA THAI PHỤ TẠI BỆNH VIỆN PHỤ SẢN TÂM PHÚC, HẢI PHÒNG`; top1 `KHẢO SÁT TỈ LỆ TRẦM CẢM VÀ CÁC YẾU TỐ LIÊN QUAN TRÊN PHỤ NỮ MANG THAI Ở 3 THÁNG CUỐI THAI KỲ TẠI BỆNH VIỆN NGUYỄN TRI PHƯƠNG`
- `q_092` expected `THÁCH THỨC TRONG CHẨN ĐOÁN VÀ ĐIỀU TRỊ BỆNH LAO Ở BỆNH NHÂN GHÉP THẬN`; top1 `ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ THOÁT VỊ BẸN BẨM SINH Ở TRẺ EM BẰNG PHẪU THUẬT NỘI SOI KHÂU KÍN ỐNG PHÚC TINH MẠC KẾT HỢP KIM ENDONEEDLE TẠI BỆNH VIỆN ĐA KHOA TỈNH NAM ĐỊNH`
- `q_094` expected `CHẨN ĐOÁN VÀ ĐIỀU TRỊ HẸP ĐỘNG MẠCH CẢNH NGOÀI SỌ`; top1 `THAY ĐỔI NHẬN THỨC VỀ ĐỘT QUỴ NÃO CỦA NGƯỜI CAO TUỔI TẠI XÃ GIAO LẠC - GIAO THỦY NAM ĐỊNH SAU CAN THIỆP GIÁO DỤC SỨC KHỎE`
- `q_095` expected `HIỆU QUẢ GIẢM ĐAU Ở THAI PHỤ CHUYỂN DẠ BẰNG GÂY TÊ NGOÀI MÀNG CỨNG NGẮT QUÃNG TỰ ĐỘNG TẠI BỆNH VIỆN ĐA KHOA TÂM ANH HÀ NỘI NĂM 2024`; top1 `MẪU HỒ SƠ MỜI THẦU MUA THUỐC ÁP DỤNG PHƯƠNG THỨC MỘT GIAI ĐOẠN MỘT TÚI HỒ SƠ - Quy định này không áp dụng đối với gói thầu thuốc biệt dược gốc hoặc tương đương điều trị.`
- `q_096` expected `NGHIÊN CỨU VAI TRÒ CỦA TROPONIN TRONG TIÊN ĐOÁN VÀ TIÊN LƯỢNG RUNG NHĨ: PHÂN TÍCH HỆ THỐNG THEO QUY TRÌNH PRISMA`; top1 `KHẢO SÁT TÌNH TRẠNG RUNG NHĨ MỚI KHỞI PHÁT VÀ MỐI LIÊN QUAN VỚI KẾT QUẢ ĐIỀU TRỊ Ở BỆNH NHÂN SỐC NHIỄM KHUẨN`
