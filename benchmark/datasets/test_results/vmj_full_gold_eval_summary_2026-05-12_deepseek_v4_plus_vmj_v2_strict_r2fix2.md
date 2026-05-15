# Full Gold vs Test Summary

- Generated at: 2026-05-12 21:22:04
- Base URL: http://localhost:8000/api/chat

## Metrics

| Split | Count | HTTP 200 | HTTP 200 Rate | Degraded | Degraded Rate | Avg Latency (ms) | Top1 Title Hit Rate (200 only) | Avg Must-Have Hit Rate (200 only) | Must-Not Violation Rate (200 only) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| dev | 64 | 64 | 100.0% | 0 | 0.0% | 10863.9 | 0.609 | 0.182 | 0.0 |
| test | 20 | 20 | 100.0% | 0 | 0.0% | 10769.1 | 0.75 | 0.218 | 0.0 |
| holdout | 18 | 18 | 100.0% | 0 | 0.0% | 11366.0 | 0.889 | 0.247 | 0.0 |
| full_gold | 102 | 102 | 100.0% | 0 | 0.0% | 10933.9 | 0.686 | 0.201 | 0.0 |

## Non-200 Cases

- None

## Retrieval Misses (Top1 Title, 200 only)

- dev q_008 gold=THOÁI HÓA KHỚP, THÁCH THỨC VỚI SỨC KHỎE NGƯỜI CAO TUỔI VÀ “CỬA SỔ CƠ HỘI” CHO ĐIỀU TRỊ top1=
- dev q_009 gold=KHẢO SÁT CÁC TRƯỜNG HỢP TRẺ SINH TỪ MẸ NHIỄM LIÊN CẦU KHUẨN NHÓM B TẠI BỆNH VIỆN ĐA KHOA TÂM ANH HỒ CHÍ MINH top1=Bảng3.4.Đặcđiểmxétnghiệmcấymáuvà trong nhóm bệnh nhân này 6. CRP tăng tại thời xétnghiệmcấycácdịchkhác điểm nhập viện chỉ ghi nhận ở 22,6% trường
- dev q_011 gold=ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT NỘI SOI CẮT TOÀN BỘ DẠ DÀY DO UNG THƯ TẠI BỆNH VIỆN UNG BƯỚU ĐÀ NẴNG top1=NGHIÊN CỨU ĐẶC ĐIỂM LÂM SÀNG, HÌNH ẢNH NỘI SOI VÀ MÔ BỆNH HỌC CỦA BỆNH NHÂN UNG THƯ DẠ DÀY DƯỚI 50 TUỔI TẠI BỆNH VIỆN UNG BƯỚU ĐÀ NẴNG
- dev q_013 gold=ỨNG DỤNG KỸ THUẬT ĐỊNH VỊ THẦN KINH TRONG PHẪU THUẬT U TẾ BÀO HÌNH SAO BẬC THẤP TẠI BỆNH VIỆN HỮU NGHỊ VIỆT ĐỨC top1=KẾT QUẢ ĐIỀU TRỊ U TẾ BÀO HÌNH SAO BẬC THẤP TRÊN LỀU CÓ SỬ DỤNG ĐỊNH VỊ THẦN KINH TẠI BỆNH VIỆN VIỆT ĐỨC
- dev q_025 gold=ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT NỘI SOI SAU PHÚC MẠC LẤY SỎI NIỆU QUẢN TẠI BỆNH VIỆN TÂM ANH GIAI ĐOẠN NĂM 2022-2024 top1=ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT NỘI SOI SAU PHÚC MẠC LẤY SỎI NIỆU QUẢN TẠI BỆNH VIỆN ĐẠI HỌC Y HÀ NỘI GIAI ĐOẠN 2020 - 2023
- dev q_026 gold=ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT NỘI SOI SAU PHÚC MẠC LẤY SỎI NIỆU QUẢN TẠI BỆNH VIỆN TÂM ANH GIAI ĐOẠN NĂM 2022-2024 top1=KHẢO SÁT KHÚC XẠ TỒN DƯ SAU PHẪU THUẬT TÁN NHUYỄN THỂ THỦY TINH TẠI BỆNH VIỆN MẮT - TAI MŨI HỌNG - RĂNG HÀM MẶT AN GIANG
- dev q_027 gold=CHẨN ĐOÁN VÀ ĐIỀU TRỊ HẸP ĐỘNG MẠCH CẢNH NGOÀI SỌ top1=ĐÁNH GIÁ KẾT QUẢ BAN ĐẦU CAN THIỆP NONG BÓNG ĐIỀU TRỊ HẸP ĐỘNG MẠCH NỘI SỌ TRÊN BỆNH NHÂN NHỒI MÁU NÃO CẤP
- dev q_028 gold=TĂNG TRƯỞNG Ở TRẺ SƠ SINH ĐƯỢC HỒI SỨC SAU PHẪU THUẬT ĐƯỜNG TIÊU HÓA TẠI BỆNH VIỆN NHI ĐỒNG 1 VÀ CÁC YẾU TỐ LIÊN QUAN top1=ĐẶC ĐIỂM HỖ TRỢ DINH DƯỠNG VÀ KẾT CỤC TĂNG TRƯỞNG Ở TRẺ SƠ SINH ĐƯỢC HỒI SỨC SAU PHẪU THUẬT ĐƯỜNG TIÊU HÓA TẠI BỆNH VIỆN NHI ĐỒNG 1
- dev q_035 gold=ĐƯỜNG CONG HỌC TẬP ĐIỀU TRỊ LỖ TIỂU THẤP THỂ GIỮA VÀ THỂ SAU BẰNG KỸ THUẬT SNODGRASS top1=NHẬN XÉT ĐẶC ĐIỂM TỔN THƯƠNG GIẢI PHẪU GÃY LIÊN MẤU CHUYỂN XƯƠNG ĐÙI TRÊN HÌNH ẢNH CHỤP CẮT LỚP VI TÍNH
- dev q_038 gold=ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ BỆNH NHÂN SUY TIM MÃN TÍNH ĐIỀU TRỊ TẠI KHOA TIM MẠCH - LÃO HỌC BỆNH VIỆN ĐA KHOA HUYỆN HOÀI ĐỨC top1=ĐÁNH GIÁ CHẤT LƯỢNG CUỘC SỐNG CỦA NGƯỜI BỆNH SUY TIM MẠN TÍNH VÀ CÁC YẾU TỐ ẢNH HƯỞNG
- dev q_039 gold=ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ BỆNH NHÂN SUY TIM MÃN TÍNH ĐIỀU TRỊ TẠI KHOA TIM MẠCH - LÃO HỌC BỆNH VIỆN ĐA KHOA HUYỆN HOÀI ĐỨC top1=HỘI NGHỊ THƯỜNG NIÊN HỌC VIÊN – SINH VIÊN NGHIÊN CỨU KHOA HỌC LẦN THỨ IV NĂM 2
- dev q_050 gold=SIÊU ÂM TẠI GIƯỜNG THEO DÕI TỔN THƯƠNG NỘI SỌ SAU PHẪU THUẬT MỞ SỌ GIẢI ÉP: KINH NGHIỆM TẠI BỆNH VIỆN ĐA KHOA XANH PÔN top1=ĐẶC ĐIỂM CHUYỂN VIỆN VÀ ĐIỀU TRỊ BAN ĐẦU Ở TRẺ TỬ VONG TRONG 24 GIỜ ĐẦU TẠI KHOA CẤP CỨU, BỆNH VIỆN NHI ĐỒNG 1
- dev q_052 gold=ĐIỀU TRỊ THOÁI HÓA KHỚP GỐI THEO THỂ BỆNH Y HỌC CỔ TRUYỀN top1=ĐÁNH GIÁ HIỆU QUẢ CỦA PHƯƠNG PHÁP ĐIỆN CHÂM KẾT HỢP SIÊU ÂM TRỊ LIỆU TRONG ĐIỀU TRỊ THOÁI HÓA KHỚP GỐI TẠI BỆNH VIỆN Y HỌC CỔ TRUYỀN NGHỆ AN
- dev q_059 gold=ĐÁNH GIÁ TÍNH AN TOÀN UNG THƯ TUYẾN GIÁP BIỆT HOÁ GIAI ĐOẠN SỚM SAU MỔ NỘI SOI CẮT THUỲ TUYẾN GIÁP NGẢ TIỀN ĐÌNH MIỆNG top1=PHẪU THUẬT TUYẾN GIÁP NỘI SOI NGẢ TIỀN ĐÌNH MIỆNG: KINH NGHIỆM TỪ BỆNH VIỆN ĐẠI HỌC Y DƯỢC TP.HCM
- dev q_062 gold=NGHIÊN CỨU ĐẶC ĐIỂM EXOSOME TỪ MÁU DÂY RỐN NGƯỜI BẰNG KĨ THUẬT TỦA POLYMER KẾT HỢP KIT HẠT TỪ MAGCAPTURE™ ỨNG DỤNG TRONG Y HỌC TÁI TẠO top1=CÔNG TRÌNH NGHIÊN CỨU KHOA HỌC TRƯỜNG ĐẠI HỌC Y DƯỢC HẢI PHÒNG
- dev q_063 gold=NGHIÊN CỨU ĐẶC ĐIỂM EXOSOME TỪ MÁU DÂY RỐN NGƯỜI BẰNG KĨ THUẬT TỦA POLYMER KẾT HỢP KIT HẠT TỪ MAGCAPTURE™ ỨNG DỤNG TRONG Y HỌC TÁI TẠO top1=CÔNG TRÌNH NGHIÊN CỨU KHOA HỌC TRƯỜNG ĐẠI HỌC Y DƯỢC HẢI PHÒNG
- dev q_071 gold=KIẾN THỨC, THÁI ĐỘ VÀ THỰC HÀNH DỰA TRÊN BẰNG CHỨNG CỦA ĐIỀU DƯỠNG TẠI MỘT BỆNH VIỆN ĐA KHOA TỈNH top1=YẾU TỐ LIÊN QUAN ĐẾN THỰC HÀNH DỰA VÀO BẰNG CHỨNG CỦA ĐIỀU DƯỠNG TẠI BỆNH VIỆN ĐA KHOA THÀNH PHỐ VINH
- dev q_076 gold=TRƯỜNG HỢP LÂM SÀNG: SỎI TUYẾN TIỀN LIỆT KHỔNG LỒ top1=PHẪU THUẬT NỘI SOI Ổ BỤNG CẮT TOÀN BỘ TUYẾN TIỀN LIỆT TRONG ĐIỀU TRỊ UNG THƯ TUYẾN TIỀN LIỆT: BÁO CÁO TRƯỜNG HỢP LÂM SÀNG
- dev q_077 gold=THẤT BẠI VỚI THÔNG KHÍ KHÔNG XÂM LẤN SAU RÚT NỘI KHÍ QUẢN top1=KẾT QUẢ THÔNG KHÍ KHÔNG XÂM LẤN SAU RÚT NỘI KHÍ QUẢN Ở TRẺ SƠ SINH NON THÁNG TẠI BỆNH VIỆN NHI ĐỒNG 1
- dev q_078 gold=THẤT BẠI VỚI THÔNG KHÍ KHÔNG XÂM LẤN SAU RÚT NỘI KHÍ QUẢN top1=KẾT QUẢ THÔNG KHÍ KHÔNG XÂM LẤN SAU RÚT NỘI KHÍ QUẢN Ở TRẺ SƠ SINH NON THÁNG TẠI BỆNH VIỆN NHI ĐỒNG 1
- dev q_081 gold=SỰ THAY ĐỔI ĐẶC ĐIỂM LÂM SÀNG TRÊN BỆNH NHÂN TRƯỞNG THÀNH BỊ RỐI LOẠN KHỚP THÁI DƯƠNG HÀM (TMD) ĐƯỢC ĐIỀU TRỊ BẰNG MÁNG ỔN ĐỊNH (SS) 6 THÁNG top1=NGHIÊN CỨU TRIỆU CHỨNG RỐI LOẠN THÁI DƯƠNG HÀM VÀ CÁC YẾU TỐ TÂM LÝ THÓI QUEN TRÊN SINH VIÊN NHA KHOA, TRƯỜNG ĐẠI HỌC Y DƯỢC CẦN THƠ
- dev q_087 gold=ĐẶC ĐIỂM LÂM SÀNG, CẬN LÂM SÀNG VÀ MỘT SỐ YẾU TỐ NGUY CƠ Ở BỆNH NHÂN SUY THƯỢNG THẬN MẠN DO LẠM DỤNG top1=CASE LÂM SÀNG: SUY THƯỢNG THẬN SAU TIÊM GLUCOCORTICOID NGOÀI MÀNG CỨNG
- dev q_091 gold=KHẢO SÁT MỘT SỐ YẾU TỐ LIÊN QUAN ĐẾN BỆNH LÝ ĐÁI THÁO ĐƯỜNG THAI KỲ CỦA THAI PHỤ TẠI BỆNH VIỆN PHỤ SẢN TÂM PHÚC, HẢI PHÒNG top1=ĐÁNH GIÁ KẾT QUẢ QUẢN LÝ RỐI LOẠN TĂNG HUYẾT ÁP THAI KỲ Ở THAI PHỤ TỪ TAM CÁ NGUYỆT THỨ HAI ĐẾN KHÁM TẠI BỆNH VIỆN CHUYÊN KHOA SẢN NHI TỈNH SÓC TRĂNG NĂM 2020-2021 Lê Thị Giáng Châu1*, Lê Thị Hoàng Mỹ
- dev q_094 gold=CHẨN ĐOÁN VÀ ĐIỀU TRỊ HẸP ĐỘNG MẠCH CẢNH NGOÀI SỌ top1=ĐÁNH GIÁ VAI TRÒ CÁC YẾU TỐ NGUY CƠ CAO TRONG PHẪU THUẬT BÓC NỘI MẠC ĐỘNG MẠCH CẢNH
- dev q_099 gold=ĐÁNH GIÁ TÍNH AN TOÀN UNG THƯ TUYẾN GIÁP BIỆT HOÁ GIAI ĐOẠN SỚM SAU MỔ NỘI SOI CẮT THUỲ TUYẾN GIÁP NGẢ TIỀN ĐÌNH MIỆNG top1=MỘT SỐ YẾU TỐ LIÊN QUAN ĐẾN CHẤT LƯỢNG CUỘC SỐNG CỦA NGƯỜI BỆNH UNG THƯ PHỔI SAU XẠ TRỊ TẠI BỆNH VIỆN PHỔI TRUNG ƯƠNG
- test q_002 gold=KHẢO SÁT MỘT SỐ YẾU TỐ LIÊN QUAN ĐẾN BỆNH LÝ ĐÁI THÁO ĐƯỜNG THAI KỲ CỦA THAI PHỤ TẠI BỆNH VIỆN PHỤ SẢN TÂM PHÚC, HẢI PHÒNG top1=KIẾN THỨC, THỰC HÀNH VÀ MỘT SỐ YẾU TỐ LIÊN QUAN ĐẾN PHÒNG NGỪA ĐÁI THÁO ĐƯỜNG THAI KỲ Ở THAI PHỤ TẠI BỆNH VIỆN SẢN NHI TRÀ VINH 2023
- test q_006 gold=ĐẶC ĐIỂM NỘI SOI, MÔ BỆNH HỌC VÀ KẾT QUẢ CẮT ĐỐT KẾT HỢP VỚI KẸP CLIP QUA NỘI SOI POLYP CÓ CUỐNG Ở ĐẠI TRỰC TRÀNG top1=đặc điểm trên bệnh nhân suy tim phân suất tống máu bảo tồn và thang điểm H2FPEF trong các nhóm nghiên cứu.
- test q_022 gold=ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ VÀ TÁC DỤNG PHỤ CỦA PHÁC ĐỒ 4 THUỐC CÓ BISMUTH PTMB TRONG DIỆT TRỪ HELICOBACTER PYLORI Ở BỆNH NHÂN LOÉT DẠ DÀY TÁ TRÀNG TẠI BỆNH VIỆN TRƯỜNG ĐẠI HỌC Y KHOA VINH top1=BẰNG PHÁC ĐỒ 4 THUỐC CÓ BISMUTH Ở BỆNH NHÂN VIÊM, LOÉT DẠ DÀY – TÁ TRÀNG TẠI BỆNH VIỆN QUÂN DÂN Y TỈNH BẠC LIÊU Di Văn Đua1, Huỳnh Hiếu Tâm2, Nguyễn Thị Quỳnh Mai3, Ngô Thị Yến Nhi4, Ngô Thị Mộng Tuyền2, Võ Tấn Trọng2, Võ Tấn Cường4* 1. Bệnh viện Quân dân Y tỉnh Bạc Liêu 2. Trường Đại học Y Dược Cần Thơ
- test q_060 gold=ĐÁNH GIÁ TÍNH AN TOÀN UNG THƯ TUYẾN GIÁP BIỆT HOÁ GIAI ĐOẠN SỚM SAU MỔ NỘI SOI CẮT THUỲ TUYẾN GIÁP NGẢ TIỀN ĐÌNH MIỆNG top1=PHẪU THUẬT TUYẾN GIÁP NỘI SOI NGẢ TIỀN ĐÌNH MIỆNG: KINH NGHIỆM TỪ BỆNH VIỆN ĐẠI HỌC Y DƯỢC TP.HCM
- test q_074 gold=LÂM SÀNG VÀ CẬN LÂM SÀNG TRƯỚC PHẪU THUẬT NỘI SOI VIÊM TÚI MẬT CẤP DO SỎI Ở NGƯỜI CAO TUỔI TẠI BỆNH VIỆN ĐA KHOA THÀNH PHỐ CẦN THƠ top1=ĐẶC ĐIỂM LÂM SÀNG VÀ CẬN LÂM SÀNG CỦA VIÊM TÚI MẬT CẤP DO SỎI Ở BỆNH NHÂN CAO TUỔI TẠI BỆNH VIỆN ĐA KHOA THÀNH PHỐ CẦN THƠ

## Low Must-Have Coverage (< 0.5, 200 only)

- dev q_005 hit_rate=0.0 question=Dựa trên nghiên cứu về bệnh lơ xê mi cấp dòng tủy ở trẻ em tại Viện Huyết học – Truyền máu Trung ương, những yếu tố kiểu hình miễn dịch nào được xem là có giá trị trong chẩn đoán và tiên lượng bệnh?
- dev q_008 hit_rate=0.0 question=Ở người cao tuổi bị thoái hóa khớp, những biện pháp can thiệp sớm nào được khuyến cáo để làm chậm tiến triển bệnh và giảm triệu chứng?
- dev q_009 hit_rate=0.0 question=Trong nghiên cứu này, dấu hiệu lâm sàng phổ biến nhất ở trẻ sơ sinh sinh từ mẹ nhiễm liên cầu khuẩn nhóm B là gì?
- dev q_013 hit_rate=0.0 question=Sau phẫu thuật u tế bào hình sao bậc thấp có sử dụng hệ thống định vị thần kinh, tỷ lệ bệnh nhân hồi phục chức năng vận động là bao nhiêu và chất lượng sống được duy trì như thế nào?
- dev q_019 hit_rate=0.2 question=Nghiên cứu về thực hành trên xác tươi nhằm đánh giá vai trò của phương pháp này đối với những khía cạnh nào của đào tạo thủ thuật/phẫu thuật?
- dev q_023 hit_rate=0.0 question=Nghiên cứu này sử dụng thiết kế nào để đánh giá hiệu quả và tác dụng phụ của phác đồ 4 thuốc có Bismuth PTMB?
- dev q_026 hit_rate=0.0 question=Theo phần đặt vấn đề của nghiên cứu, vì sao phẫu thuật nội soi sau phúc mạc vẫn giữ vai trò quan trọng trong điều trị sỏi niệu quản?
- dev q_027 hit_rate=0.4 question=Dựa trên thông tin này, liệu việc điều trị nội khoa tối ưu và kiểm soát yếu tố nguy cơ tim mạch sau can thiệp có đảm bảo loại bỏ hoàn toàn nguy cơ tái hẹp và đột quỵ không?
- dev q_031 hit_rate=0.0 question=Theo nghiên cứu này, những yếu tố nào có liên quan đến kiến thức, thái độ hoặc thực hành về sức khỏe sinh sản ở học sinh trung học?
- dev q_034 hit_rate=0.25 question=Theo nghiên cứu này, việc tiêu thụ cơm có tác động như thế nào đến nguy cơ suy dinh dưỡng ở bệnh nhân nội trú so với các loại thức ăn mềm khác?
- dev q_035 hit_rate=0.0 question=Trong nghiên cứu về đường cong học tập kỹ thuật Snodgrass, các trường hợp được chia như thế nào để phân tích ảnh hưởng của kinh nghiệm phẫu thuật viên đến biến chứng?
- dev q_038 hit_rate=0.0 question=Theo nghiên cứu này, kết quả điều trị ở bệnh nhân suy tim mạn tính được đánh giá qua những chỉ số hoặc tiêu chí nào?
- dev q_039 hit_rate=0.0 question=Trong nghiên cứu này, bệnh nhân suy tim mạn tính được can thiệp và theo dõi nhằm cải thiện những khía cạnh điều trị nào?
- dev q_040 hit_rate=0.2 question=Trong trường hợp bệnh nhân đã mắc rung nhĩ, đặc biệt là tại phòng cấp cứu, nồng độ troponin tăng cao có ý nghĩa tiên lượng như thế nào đối với sức khỏe tim mạch?
- dev q_043 hit_rate=0.0 question=Dựa trên nghiên cứu này, ngưỡng tiểu cầu ≤ 20 × 10³/µL có thể được sử dụng làm giá trị báo động lâm sàng để quyết định truyền tiểu cầu cho tất cả các bệnh nhân tại Bệnh viện Nhi Đồng 1 hay không?
- dev q_045 hit_rate=0.3333333333333333 question=Những đặc điểm cận lâm sàng nào có sự khác biệt có ý nghĩa thống kê giữa nhiễm khuẩn huyết do vi khuẩn gram âm và nhiễm khuẩn huyết do vi khuẩn gram dương theo kết quả nghiên cứu?
- dev q_046 hit_rate=0.0 question=Dựa trên nghiên cứu này, phẫu thuật nội soi sau phúc mạc cắt bướu tuyến thượng thận có được coi là phương pháp điều trị duy nhất được áp dụng cho tất cả các trường hợp bướu tuyến thượng thận không?
- dev q_047 hit_rate=0.0 question=Trong các trường hợp trượt đốt sống thắt lưng nặng được báo cáo, những thách thức nào được đặt ra khi lựa chọn phương pháp phẫu thuật?
- dev q_048 hit_rate=0.3333333333333333 question=Nghiên cứu về điều trị rối loạn khớp thái dương hàm dưới bằng máng nhai ổn định có sử dụng T-scan được thực hiện với thiết kế nào và tại cơ sở nào?
- dev q_050 hit_rate=0.0 question=Vì sao việc vận chuyển bệnh nhân hồi sức tích cực đến phòng chụp cắt lớp vi tính sọ não lại tiềm ẩn nhiều rủi ro và khó khăn?
- dev q_051 hit_rate=0.2 question=Những ưu điểm của kỹ thuật siêu âm tại giường so với chụp cắt lớp vi tính trong việc theo dõi tổn thương nội sọ ở bệnh nhân sau phẫu thuật mở sọ giải ép là gì?
- dev q_052 hit_rate=0.0 question=Dựa trên nghiên cứu này, liệu pháp siêu âm kết hợp điện châm có hiệu quả điều trị thoái hóa khớp gối tương đương nhau ở các thể bệnh y học cổ truyền khác nhau không?
- dev q_053 hit_rate=0.0 question=Trong trường hợp bệnh nhân xơ cứng bì gặp biến chứng cơn bão thận, những dấu hiệu lâm sàng nào thường xuất hiện?
- dev q_055 hit_rate=0.2 question=Những nguyên nhân chính nào dẫn đến việc tăng tỷ lệ mổ lấy thai ở nhóm thai phụ mang thai to so với nhóm thai bình thường theo nghiên cứu này?
- dev q_056 hit_rate=0.0 question=Kết quả nghiên cứu tại Bệnh viện Đa khoa tỉnh Vĩnh Long chỉ ra những biến chứng sản khoa nào có tỷ lệ cao hơn đáng kể ở thai phụ mang thai to so với thai phụ có thai nhi cân nặng bình thường?
- dev q_059 hit_rate=0.0 question=Dựa trên nghiên cứu này, tại sao phẫu thuật nội soi cắt tuyến giáp qua ngả tiền đình miệng lại được coi là một lựa chọn khả thi và an toàn cho bệnh nhân ung thư tuyến giáp biệt hóa giai đoạn sớm?
- dev q_062 hit_rate=0.0 question=Những cytokine và yếu tố tăng trưởng nào được tìm thấy chủ yếu trong exosome từ huyết tương giàu tiểu cầu (PRP) theo nghiên cứu này?
- dev q_066 hit_rate=0.0 question=Nghiên cứu này được thực hiện nhằm xác định tỷ lệ suy giáp và những yếu tố dự đoán nào ở bệnh nhân đái tháo đường típ 2 có bệnh thận mạn?
- dev q_067 hit_rate=0.3333333333333333 question=Những yếu tố nào liên quan đến tình trạng hạ đường huyết về đêm ở bệnh nhân đái tháo đường týp 2 theo nghiên cứu này?
- dev q_069 hit_rate=0.0 question=Nghiên cứu so sánh đường hầm nhỏ và đường hầm tiêu chuẩn trong điều trị sỏi san hô được thực hiện trên nhóm bệnh nhân nào và trong giai đoạn nào?

## Must-Not Violations (200 only)

- None

## Slowest 15

- dev q_008 latency_ms=37375.6 status=200
- holdout q_012 latency_ms=18061.5 status=200
- holdout q_068 latency_ms=17547.8 status=200
- dev q_094 latency_ms=17010.2 status=200
- dev q_003 latency_ms=17002.8 status=200
- dev q_092 latency_ms=16814.7 status=200
- dev q_046 latency_ms=16469.7 status=200
- dev q_098 latency_ms=16356.0 status=200
- test q_074 latency_ms=16259.4 status=200
- holdout q_097 latency_ms=15841.2 status=200
- test q_064 latency_ms=15748.5 status=200
- dev q_019 latency_ms=15505.0 status=200
- dev q_009 latency_ms=15354.9 status=200
- holdout q_073 latency_ms=15247.0 status=200
- dev q_076 latency_ms=15199.8 status=200
