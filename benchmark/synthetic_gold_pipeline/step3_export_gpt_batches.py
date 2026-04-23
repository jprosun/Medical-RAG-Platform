# -*- coding: utf-8 -*-
"""
Export items to batch TXT files for manual ChatGPT copy-pasting.
Each TXT file will contain the system prompt + 10 items (questions + contexts).
"""
import json
import os

TEMPLATE_FILE = os.path.join(os.path.dirname(__file__), "output", "step3_annotation_template.jsonl")
PROMPT_FILE = os.path.join(os.path.dirname(__file__), "prompt_step3.txt")
BATCH_DIR = os.path.join(os.path.dirname(__file__), "output", "chatgpt_batches")

BATCH_SIZE = 10

def main():
    if not os.path.exists(BATCH_DIR):
        os.makedirs(BATCH_DIR)
        
    with open(PROMPT_FILE, "r", encoding="utf-8") as f:
        base_prompt = f.read().strip()
        
    # Sửa nhẹ câu yêu cầu cuối của prompt để ChatGPT hiểu là phải trả về Mảng JSON (nhiều item) thay vì 1 object
    base_prompt = base_prompt.replace(
        "Trả về ĐÚNG 1 JSON object với schema:",
        "Trả về MỘT MẢNG (JSON array) gồm các object, mỗi object CẦN CÓ THÊM trường 'query_id' để map lại với đầu vào:"
    )
    base_prompt = base_prompt.replace(
        "{\n  \"ground_truth\": \"...\",\n  \"short_answer\": \"...\",\n  \"must_have_concepts\": [\"...\", \"...\"],\n  \"must_not_claim\": [\"...\", \"...\"]\n}",
        "[\n  {\n    \"query_id\": \"MÃ_CÂU_HỎI\",\n    \"ground_truth\": \"...\",\n    \"short_answer\": \"...\",\n    \"must_have_concepts\": [\"...\", \"...\"],\n    \"must_not_claim\": [\"...\", \"...\"]\n  }\n]"
    )
        
    items = []
    with open(TEMPLATE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                items.append(json.loads(line))
                
    total_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(total_batches):
        batch_items = items[i*BATCH_SIZE : (i+1)*BATCH_SIZE]
        
        # Chỉ truyền cho ChatGPT: ID, Câu hỏi, Behavior mong muốn, và Context
        input_data = []
        for item in batch_items:
            input_data.append({
                "query_id": item["query_id"],
                "question": item["question"],
                "expected_behavior": item["expected_behavior"],
                "context": item["context"]
            })
            
        file_content = f"{base_prompt}\n\n"
        file_content += "========================================\n"
        file_content += f"DỮ LIỆU ĐẦU VÀO (BATCH {i+1}/{total_batches})\n"
        file_content += f"Lưu ý quan trọng: Dưới đây là {len(batch_items)} câu hỏi. Trả về đúng 1 mảng (JSON array) chứa {len(batch_items)} kết quả tương ứng với từng query_id.\n\n"
        file_content += "```json\n"
        file_content += json.dumps(input_data, ensure_ascii=False, indent=2)
        file_content += "\n```\n"
        
        batch_filename = os.path.join(BATCH_DIR, f"batch_{i+1:02d}.txt")
        with open(batch_filename, "w", encoding="utf-8") as f:
            f.write(file_content)
            
    print(f"✅ Đã chia {len(items)} câu hỏi thành {total_batches} file batch.")
    print(f"📂 Thư mục chứa: {BATCH_DIR}")

if __name__ == "__main__":
    main()
