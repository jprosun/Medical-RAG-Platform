# Full Gold vs Test Summary

- Generated at: 2026-04-22 18:22:55
- Base URL: http://127.0.0.1:8000/api/chat

## Metrics

| Split | Count | HTTP 200 | HTTP 200 Rate | Degraded | Degraded Rate | Avg Latency (ms) | Top1 Title Hit Rate (200 only) | Avg Must-Have Hit Rate (200 only) | Must-Not Violation Rate (200 only) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| dev | 64 | 64 | 100.0% | 0 | 0.0% | 12820.6 | 0.734 | 0.247 | 0.0 |
| test | 20 | 20 | 100.0% | 0 | 0.0% | 20427.1 | 0.6 | 0.217 | 0.0 |
| holdout | 18 | 18 | 100.0% | 0 | 0.0% | 14539.8 | 0.889 | 0.147 | 0.0 |
| full_gold | 102 | 102 | 100.0% | 0 | 0.0% | 14615.5 | 0.735 | 0.224 | 0.0 |

## Non-200 Cases

- None

## Retrieval Misses (Top1 Title, 200 only)

- dev q_011 gold=ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT NỘI SOI CẮT TOÀN BỘ DẠ DÀY DO UNG THƯ TẠI BỆNH VIỆN UNG BƯỚU ĐÀ NẴNG top1=NGHIÊN CỨU ĐẶC ĐIỂM LÂM SÀNG, HÌNH ẢNH NỘI SOI VÀ MÔ BỆNH HỌC CỦA BỆNH NHÂN UNG THƯ DẠ DÀY DƯỚI 50 TUỔI TẠI BỆNH VIỆN UNG BƯỚU ĐÀ NẴNG
- dev q_025 gold=ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT NỘI SOI SAU PHÚC MẠC LẤY SỎI NIỆU QUẢN TẠI BỆNH VIỆN TÂM ANH GIAI ĐOẠN NĂM 2022-2024 top1=ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT NỘI SOI SAU PHÚC MẠC LẤY SỎI NIỆU QUẢN TẠI BỆNH VIỆN TÂM ANH GIAI ĐOẠN NĂM 2022- 2
- dev q_026 gold=ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT NỘI SOI SAU PHÚC MẠC LẤY SỎI NIỆU QUẢN TẠI BỆNH VIỆN TÂM ANH GIAI ĐOẠN NĂM 2022-2024 top1=PHẪU THUẬT NỘI SOI SAU PHÚC MẠC CẮT TUYẾN THƯỢNG THẬN TƯ THẾ NẰM SẤP
- dev q_027 gold=CHẨN ĐOÁN VÀ ĐIỀU TRỊ HẸP ĐỘNG MẠCH CẢNH NGOÀI SỌ top1=ĐÁNH GIÁ KẾT QUẢ BAN ĐẦU CAN THIỆP NONG BÓNG ĐIỀU TRỊ HẸP ĐỘNG MẠCH NỘI SỌ TRÊN BỆNH NHÂN NHỒI MÁU NÃO CẤP
- dev q_039 gold=ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ BỆNH NHÂN SUY TIM MÃN TÍNH ĐIỀU TRỊ TẠI KHOA TIM MẠCH - LÃO HỌC BỆNH VIỆN ĐA KHOA HUYỆN HOÀI ĐỨC top1=THỰC TRẠNG TUÂN THỦ ĐIỀU TRỊ THUỐC NGOẠI TRÚ VÀ MỘT SỐ YẾU TỐ LIÊN QUAN Ở BỆNH NHÂN SUY TIM MẠN TÍNH TẠI BỆNH VIỆN HỮU NGHỊ VIỆT TIỆP
- dev q_050 gold=SIÊU ÂM TẠI GIƯỜNG THEO DÕI TỔN THƯƠNG NỘI SỌ SAU PHẪU THUẬT MỞ SỌ GIẢI ÉP: KINH NGHIỆM TẠI BỆNH VIỆN ĐA KHOA XANH PÔN top1=CÁC YẾU TỐ LIÊN QUAN ĐẾN KẾT QUẢ HỒI SỨC TIM PHỔI Ở BỆNH NHÂN NGỪNG TUẦN HOÀN NGOẠI VIỆN VÀO KHOA CẤP CỨU BỆNH VIỆN HỮU NGHỊ ĐA KHOA NGHỆ AN
- dev q_052 gold=ĐIỀU TRỊ THOÁI HÓA KHỚP GỐI THEO THỂ BỆNH Y HỌC CỔ TRUYỀN top1=ĐÁNH GIÁ HIỆU QUẢ CỦA PHƯƠNG PHÁP ĐIỆN CHÂM KẾT HỢP SIÊU ÂM TRỊ LIỆU TRONG ĐIỀU TRỊ THOÁI HÓA KHỚP GỐI TẠI BỆNH VIỆN Y HỌC CỔ TRUYỀN NGHỆ AN
- dev q_054 gold=ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ BAN ĐẦU BỆNH COATS TẠI BỆNH VIỆN MẮT TRUNG ƯƠNG top1=Asean, trang 243-248. 6. Trần Ngọc Bích, Trần Ngọc Sơn. Điều trị phẫu thuật hẹp niệu đạo sau mổ lỗ tiểu thấp: kinh nghiệm ở 49 bệnh nhân. Tạp chí Nghiên cứu và Thực hành Nhi khoa - BV Nhi trung Ương 2022, 6 (tập 3+4), 122–130.
- dev q_059 gold=ĐÁNH GIÁ TÍNH AN TOÀN UNG THƯ TUYẾN GIÁP BIỆT HOÁ GIAI ĐOẠN SỚM SAU MỔ NỘI SOI CẮT THUỲ TUYẾN GIÁP NGẢ TIỀN ĐÌNH MIỆNG top1=PHẪU THUẬT TUYẾN GIÁP NỘI SOI NGẢ TIỀN ĐÌNH MIỆNG: KINH NGHIỆM TỪ BỆNH VIỆN ĐẠI HỌC Y DƯỢC TP.HCM
- dev q_062 gold=NGHIÊN CỨU ĐẶC ĐIỂM EXOSOME TỪ MÁU DÂY RỐN NGƯỜI BẰNG KĨ THUẬT TỦA POLYMER KẾT HỢP KIT HẠT TỪ MAGCAPTURE™ ỨNG DỤNG TRONG Y HỌC TÁI TẠO top1=CÔNG TRÌNH NGHIÊN CỨU KHOA HỌC TRƯỜNG ĐẠI HỌC Y DƯỢC HẢI PHÒNG
- dev q_063 gold=NGHIÊN CỨU ĐẶC ĐIỂM EXOSOME TỪ MÁU DÂY RỐN NGƯỜI BẰNG KĨ THUẬT TỦA POLYMER KẾT HỢP KIT HẠT TỪ MAGCAPTURE™ ỨNG DỤNG TRONG Y HỌC TÁI TẠO top1=CÔNG TRÌNH NGHIÊN CỨU KHOA HỌC TRƯỜNG ĐẠI HỌC Y DƯỢC HẢI PHÒNG
- dev q_066 gold=TỶ LỆ VÀ CÁC YẾU TỐ DỰ ĐOÁN SUY GIÁP Ở BỆNH NHÂN ĐÁI THÁO ĐƯỜNG TÍP 2 CÓ BỆNH THẬN MẠN top1=chụp cắt lớp vi tính trong chẩn đoán viêm ruột thừa cấp, Luận văn Thạc sỹ Y học, Trường Đại học Y Hà Nội, Hà Nội.
- dev q_067 gold=HẠ ĐƯỜNG HUYẾT VỀ ĐÊM Ở BỆNH NHÂN ĐÁI THÁO ĐƯỜNG TÝP 2: VAI TRÒ CỦA THIẾT BỊ THEO DÕI ĐƯỜNG HUYẾT LIÊN TỤC top1=2. Bộ Y tế Quyết định 5481/QĐ-BYT. Quyết định về việc ban hành tài liệu chuyên môn “Hướng dẫn chẩn đoán và điều trị đái tháo đường type 2”. 2020. 3. Trần Thị Thùy Linh, Trần Ngọc Dung. Tỷ lệ biến
- dev q_071 gold=KIẾN THỨC, THÁI ĐỘ VÀ THỰC HÀNH DỰA TRÊN BẰNG CHỨNG CỦA ĐIỀU DƯỠNG TẠI MỘT BỆNH VIỆN ĐA KHOA TỈNH top1=YẾU TỐ LIÊN QUAN ĐẾN THỰC HÀNH DỰA VÀO BẰNG CHỨNG CỦA ĐIỀU DƯỠNG TẠI BỆNH VIỆN ĐA KHOA THÀNH PHỐ VINH
- dev q_078 gold=THẤT BẠI VỚI THÔNG KHÍ KHÔNG XÂM LẤN SAU RÚT NỘI KHÍ QUẢN top1=CHẬM TĂNG TRƯỞNG NGOÀI TỬ CUNG Ở TRẺ SƠ SINH NON THÁNG TẠI BỆNH VIỆN NHI ĐỒNG 1 VÀ CÁC YẾU TỐ LIÊN QUAN
- dev q_087 gold=ĐẶC ĐIỂM LÂM SÀNG, CẬN LÂM SÀNG VÀ MỘT SỐ YẾU TỐ NGUY CƠ Ở BỆNH NHÂN SUY THƯỢNG THẬN MẠN DO LẠM DỤNG top1=CASE LÂM SÀNG: SUY THƯỢNG THẬN SAU TIÊM GLUCOCORTICOID NGOÀI MÀNG CỨNG
- dev q_099 gold=ĐÁNH GIÁ TÍNH AN TOÀN UNG THƯ TUYẾN GIÁP BIỆT HOÁ GIAI ĐOẠN SỚM SAU MỔ NỘI SOI CẮT THUỲ TUYẾN GIÁP NGẢ TIỀN ĐÌNH MIỆNG top1=lâm sàng và kết quả điều trị viêm phổi sơ sinh tại đồng, 2022. 63(Số chuyên đề 1-BV Nhi Thái Bình). 7. Green, R.J. and J.M. Kolberg, Neonatal
- test q_002 gold=KHẢO SÁT MỘT SỐ YẾU TỐ LIÊN QUAN ĐẾN BỆNH LÝ ĐÁI THÁO ĐƯỜNG THAI KỲ CỦA THAI PHỤ TẠI BỆNH VIỆN PHỤ SẢN TÂM PHÚC, HẢI PHÒNG top1=NGHIÊN CỨU KẾT CỤC THAI KỲ Ở SẢN PHỤ MẮC ĐÁI THÁO ĐƯỜNG
- test q_004 gold=ĐỒNG MẮC CARCINÔM TUYẾN Ở BUỒNG TRỨNG TÁI PHÁT CÓ DI CĂN XA VÀ U DIỆP THỂ ÁC HIỆN DIỆN THÀNH PHẦN BIỆT HÓA SARCÔM DỊ LOẠI Ở VÚ: BÁO CÁO MỘT TRƯỜNG HỢP HIẾM GẶP top1=MỘT SỐ ĐẶC ĐIỂM MÔ BỆNH HỌC UNG THƯ BIỂU MÔ VÚ DỊ SẢN TẠI BỆNH VIỆN K
- test q_006 gold=ĐẶC ĐIỂM NỘI SOI, MÔ BỆNH HỌC VÀ KẾT QUẢ CẮT ĐỐT KẾT HỢP VỚI KẸP CLIP QUA NỘI SOI POLYP CÓ CUỐNG Ở ĐẠI TRỰC TRÀNG top1=ĐẶC ĐIỂM NỘI SOI, MÔ BỆNH HỌC V[ KẾT QUẢ CẮT ĐỐT KẾT HỢP VỚI KẸP CLIP QUA NỘI SOI POLYP B\N CUỐNG Ở ĐẠI TRỰC TR[NG
- test q_022 gold=ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ VÀ TÁC DỤNG PHỤ CỦA PHÁC ĐỒ 4 THUỐC CÓ BISMUTH PTMB TRONG DIỆT TRỪ HELICOBACTER PYLORI Ở BỆNH NHÂN LOÉT DẠ DÀY TÁ TRÀNG TẠI BỆNH VIỆN TRƯỜNG ĐẠI HỌC Y KHOA VINH top1=ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ LOÉT DẠ DÀY TÁ TRÀNG CÓ NHIỄM HELICOBACTER PYLORI BẰNG PHÁC ĐỒ BỐN THUỐC CÓ BISMUTH TẠI KHOA NỘI TIÊU HOÁ BỆNH VIỆN TRUNG ƯƠNG THÁI NGUYÊN
- test q_033 gold=TẦN SUẤT SUY DINH DƯỠNG THEO GLIM VÀ CÁC YẾU TỐ LIÊN QUAN Ở BỆNH NHÂN ĐIỀU TRỊ NỘI TRÚ TẠI BỆNH VIỆN THỐNG NHẤT NĂM 2025 top1=TÌNH TRẠNG DINH DƯỠNG VÀ KHẨU PHẦN NUÔI DƯỠNG CỦA NGƯỜI BỆNH CAO TUỔI TẠI KHOA HỒI SỨC TÍCH CỰC VÀ CHỐNG ĐỘC, BỆNH VIỆN HỮU NGHỊ NĂM 2024 – 2
- test q_060 gold=ĐÁNH GIÁ TÍNH AN TOÀN UNG THƯ TUYẾN GIÁP BIỆT HOÁ GIAI ĐOẠN SỚM SAU MỔ NỘI SOI CẮT THUỲ TUYẾN GIÁP NGẢ TIỀN ĐÌNH MIỆNG top1=PHẪU THUẬT TUYẾN GIÁP NỘI SOI NGẢ TIỀN ĐÌNH MIỆNG: KINH NGHIỆM TỪ BỆNH VIỆN ĐẠI HỌC Y DƯỢC TP.HCM
- test q_074 gold=LÂM SÀNG VÀ CẬN LÂM SÀNG TRƯỚC PHẪU THUẬT NỘI SOI VIÊM TÚI MẬT CẤP DO SỎI Ở NGƯỜI CAO TUỔI TẠI BỆNH VIỆN ĐA KHOA THÀNH PHỐ CẦN THƠ top1=Chất lượng cuộc sống của bệnh nhân được do trước mổ thang điểm đánh giá có mức điểm thấp hơn, chất lượng cuộc sống tốt hơn nên sau
- test q_101 gold=NGHIÊN CỨU ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ PHỤC HỒI TỔN THƯƠNG RĂNG CỐI LỚN BẰNG INLAY SỨ LAI CÓ ỨNG DỤNG KỸ THUẬT SỐ TRONG LẤY DẤU VÀ CHẾ TÁC TẠI BỆNH VIỆN RĂNG HÀM MẶT top1=nội soi ổ bụng, bệnh viện Hữu Nghị Hà Nội”. nội soi” Gây mê hồi sức cho phẫu thuật nội soi. Nhà xuất bản Giáo dục Việt Nam, tr 48-49.
- holdout q_007 gold=ĐẶC ĐIỂM NỘI SOI, MÔ BỆNH HỌC VÀ KẾT QUẢ CẮT ĐỐT KẾT HỢP VỚI KẸP CLIP QUA NỘI SOI POLYP CÓ CUỐNG Ở ĐẠI TRỰC TRÀNG top1=ĐẶC ĐIỂM NỘI SOI, MÔ BỆNH HỌC V[ KẾT QUẢ CẮT ĐỐT KẾT HỢP VỚI KẸP CLIP QUA NỘI SOI POLYP B\N CUỐNG Ở ĐẠI TRỰC TR[NG
- holdout q_072 gold=ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT HẸP ỐNG SỐNG THẮT LƯNG CÙNG CÓ MẤT VỮNG DO THOÁI HÓA Ở NGƯỜI LỚN TUỔI top1=một số yếu tố liên quan từ phía người bệnh đến kết quả điều trị. Về hiệu quả điều trị chung chúng tôi chia thành hai nhóm là Chất lượng giấc

## Low Must-Have Coverage (< 0.5, 200 only)

- dev q_003 hit_rate=0.3333333333333333 question=Đối với u tương bào ngoài tủy đơn độc ở phổi, phương pháp điều trị nào được ưu tiên lựa chọn cho các tổn thương cô lập?
- dev q_005 hit_rate=0.0 question=Dựa trên nghiên cứu về bệnh lơ xê mi cấp dòng tủy ở trẻ em tại Viện Huyết học – Truyền máu Trung ương, những yếu tố kiểu hình miễn dịch nào được xem là có giá trị trong chẩn đoán và tiên lượng bệnh?
- dev q_008 hit_rate=0.4 question=Ở người cao tuổi bị thoái hóa khớp, những biện pháp can thiệp sớm nào được khuyến cáo để làm chậm tiến triển bệnh và giảm triệu chứng?
- dev q_013 hit_rate=0.3333333333333333 question=Sau phẫu thuật u tế bào hình sao bậc thấp có sử dụng hệ thống định vị thần kinh, tỷ lệ bệnh nhân hồi phục chức năng vận động là bao nhiêu và chất lượng sống được duy trì như thế nào?
- dev q_017 hit_rate=0.25 question=Dựa trên nghiên cứu được thực hiện tại quận Phú Nhuận năm 2023, liệu kết quả quản lý hoạt động mua bán thuốc có thể đại diện cho tình hình chung của tất cả các nhà thuốc tư nhân tại Thành phố Hồ Chí Minh không?
- dev q_019 hit_rate=0.2 question=Nghiên cứu về thực hành trên xác tươi nhằm đánh giá vai trò của phương pháp này đối với những khía cạnh nào của đào tạo thủ thuật/phẫu thuật?
- dev q_023 hit_rate=0.0 question=Nghiên cứu này sử dụng thiết kế nào để đánh giá hiệu quả và tác dụng phụ của phác đồ 4 thuốc có Bismuth PTMB?
- dev q_026 hit_rate=0.0 question=Theo phần đặt vấn đề của nghiên cứu, vì sao phẫu thuật nội soi sau phúc mạc vẫn giữ vai trò quan trọng trong điều trị sỏi niệu quản?
- dev q_027 hit_rate=0.4 question=Dựa trên thông tin này, liệu việc điều trị nội khoa tối ưu và kiểm soát yếu tố nguy cơ tim mạch sau can thiệp có đảm bảo loại bỏ hoàn toàn nguy cơ tái hẹp và đột quỵ không?
- dev q_031 hit_rate=0.0 question=Theo nghiên cứu này, những yếu tố nào có liên quan đến kiến thức, thái độ hoặc thực hành về sức khỏe sinh sản ở học sinh trung học?
- dev q_034 hit_rate=0.0 question=Theo nghiên cứu này, việc tiêu thụ cơm có tác động như thế nào đến nguy cơ suy dinh dưỡng ở bệnh nhân nội trú so với các loại thức ăn mềm khác?
- dev q_035 hit_rate=0.25 question=Trong nghiên cứu về đường cong học tập kỹ thuật Snodgrass, các trường hợp được chia như thế nào để phân tích ảnh hưởng của kinh nghiệm phẫu thuật viên đến biến chứng?
- dev q_038 hit_rate=0.0 question=Theo nghiên cứu này, kết quả điều trị ở bệnh nhân suy tim mạn tính được đánh giá qua những chỉ số hoặc tiêu chí nào?
- dev q_039 hit_rate=0.2 question=Trong nghiên cứu này, bệnh nhân suy tim mạn tính được can thiệp và theo dõi nhằm cải thiện những khía cạnh điều trị nào?
- dev q_040 hit_rate=0.2 question=Trong trường hợp bệnh nhân đã mắc rung nhĩ, đặc biệt là tại phòng cấp cứu, nồng độ troponin tăng cao có ý nghĩa tiên lượng như thế nào đối với sức khỏe tim mạch?
- dev q_043 hit_rate=0.0 question=Dựa trên nghiên cứu này, ngưỡng tiểu cầu ≤ 20 × 10³/µL có thể được sử dụng làm giá trị báo động lâm sàng để quyết định truyền tiểu cầu cho tất cả các bệnh nhân tại Bệnh viện Nhi Đồng 1 hay không?
- dev q_045 hit_rate=0.3333333333333333 question=Những đặc điểm cận lâm sàng nào có sự khác biệt có ý nghĩa thống kê giữa nhiễm khuẩn huyết do vi khuẩn gram âm và nhiễm khuẩn huyết do vi khuẩn gram dương theo kết quả nghiên cứu?
- dev q_046 hit_rate=0.0 question=Dựa trên nghiên cứu này, phẫu thuật nội soi sau phúc mạc cắt bướu tuyến thượng thận có được coi là phương pháp điều trị duy nhất được áp dụng cho tất cả các trường hợp bướu tuyến thượng thận không?
- dev q_047 hit_rate=0.3333333333333333 question=Trong các trường hợp trượt đốt sống thắt lưng nặng được báo cáo, những thách thức nào được đặt ra khi lựa chọn phương pháp phẫu thuật?
- dev q_048 hit_rate=0.0 question=Nghiên cứu về điều trị rối loạn khớp thái dương hàm dưới bằng máng nhai ổn định có sử dụng T-scan được thực hiện với thiết kế nào và tại cơ sở nào?
- dev q_050 hit_rate=0.0 question=Vì sao việc vận chuyển bệnh nhân hồi sức tích cực đến phòng chụp cắt lớp vi tính sọ não lại tiềm ẩn nhiều rủi ro và khó khăn?
- dev q_051 hit_rate=0.0 question=Những ưu điểm của kỹ thuật siêu âm tại giường so với chụp cắt lớp vi tính trong việc theo dõi tổn thương nội sọ ở bệnh nhân sau phẫu thuật mở sọ giải ép là gì?
- dev q_052 hit_rate=0.0 question=Dựa trên nghiên cứu này, liệu pháp siêu âm kết hợp điện châm có hiệu quả điều trị thoái hóa khớp gối tương đương nhau ở các thể bệnh y học cổ truyền khác nhau không?
- dev q_053 hit_rate=0.0 question=Trong trường hợp bệnh nhân xơ cứng bì gặp biến chứng cơn bão thận, những dấu hiệu lâm sàng nào thường xuất hiện?
- dev q_055 hit_rate=0.2 question=Những nguyên nhân chính nào dẫn đến việc tăng tỷ lệ mổ lấy thai ở nhóm thai phụ mang thai to so với nhóm thai bình thường theo nghiên cứu này?
- dev q_056 hit_rate=0.0 question=Kết quả nghiên cứu tại Bệnh viện Đa khoa tỉnh Vĩnh Long chỉ ra những biến chứng sản khoa nào có tỷ lệ cao hơn đáng kể ở thai phụ mang thai to so với thai phụ có thai nhi cân nặng bình thường?
- dev q_059 hit_rate=0.25 question=Dựa trên nghiên cứu này, tại sao phẫu thuật nội soi cắt tuyến giáp qua ngả tiền đình miệng lại được coi là một lựa chọn khả thi và an toàn cho bệnh nhân ung thư tuyến giáp biệt hóa giai đoạn sớm?
- dev q_065 hit_rate=0.25 question=Trong nghiên cứu tại Bệnh viện Y học cổ truyền Bộ Công an, với bệnh nhân gút mạn tính có tăng acid uric máu, những phương pháp điều trị nào đã được so sánh để đánh giá hiệu quả?
- dev q_066 hit_rate=0.0 question=Nghiên cứu này được thực hiện nhằm xác định tỷ lệ suy giáp và những yếu tố dự đoán nào ở bệnh nhân đái tháo đường típ 2 có bệnh thận mạn?
- dev q_067 hit_rate=0.3333333333333333 question=Những yếu tố nào liên quan đến tình trạng hạ đường huyết về đêm ở bệnh nhân đái tháo đường týp 2 theo nghiên cứu này?

## Must-Not Violations (200 only)

- None

## Slowest 15

- test q_060 latency_ms=77073.6 status=200
- dev q_084 latency_ms=51921.7 status=200
- holdout q_068 latency_ms=41963.9 status=200
- test q_064 latency_ms=37685.8 status=200
- test q_036 latency_ms=29758.4 status=200
- dev q_066 latency_ms=29075.2 status=200
- dev q_031 latency_ms=26963.5 status=200
- test q_058 latency_ms=26726.1 status=200
- dev q_076 latency_ms=26177.0 status=200
- dev q_034 latency_ms=24864.8 status=200
- test q_074 latency_ms=23994.4 status=200
- dev q_005 latency_ms=22934.3 status=200
- test q_061 latency_ms=22597.9 status=200
- dev q_019 latency_ms=22034.4 status=200
- holdout q_097 latency_ms=21917.3 status=200
