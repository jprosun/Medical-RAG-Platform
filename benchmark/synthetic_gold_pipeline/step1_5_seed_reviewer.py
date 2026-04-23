import streamlit as st
import json
import os

INPUT_FILE = "benchmark/synthetic_gold_pipeline/output/seed_contexts_vmj_v1.jsonl"
OUTPUT_FILE = "benchmark/synthetic_gold_pipeline/output/seed_contexts_vmj_v1_whitelisted.jsonl"

st.set_page_config(layout="wide", page_title="Seed Context Reviewer")

@st.cache_data
def load_data():
    if not os.path.exists(INPUT_FILE):
        return []
    data = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data

def save_data(data):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for row in data:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

records = load_data()

if "decisions" not in st.session_state:
    # Try to load existing decisions if output file exists
    existing = {}
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    r = json.loads(line)
                    existing[r["chunk_id"]] = True
    
    st.session_state.decisions = existing

st.title("Step 1.5: Seed Context Manual Whitelist")
st.markdown("Đánh giá 120 seed contexts. Hãy chọn ra đúng 80 seed hoàn hảo nhất để đi tiếp vào Step 2.")

col1, col2 = st.columns([1, 1])
accepted_count = sum(1 for v in st.session_state.decisions.values() if v)
col1.metric("Tổng số Seed đã nạp", len(records))
col2.metric("Số Seed được Accept (Mục tiêu: 80)", accepted_count)

if st.button("Lưu danh sách đã Accept (Whitelisted)"):
    whitelisted = [r for r in records if st.session_state.decisions.get(r["chunk_id"])]
    save_data(whitelisted)
    st.success(f"Đã lưu {len(whitelisted)} chunks vào {OUTPUT_FILE}!")

st.markdown("---")

for i, record in enumerate(records):
    cid = record["chunk_id"]
    is_accepted = st.session_state.decisions.get(cid, False)
    
    with st.expander(f"{'✅' if is_accepted else '⬜'} Seed {i+1}: {record.get('title', 'No Title')} (Topic: {record.get('topic', 'unknown')})", expanded=not is_accepted):
        c_left, c_right = st.columns([3, 1])
        with c_left:
            st.markdown(f"**Doc ID:** {record.get('doc_id')}")
            st.markdown(f"**Context Text:**\n\n{record.get('context')}")
            
        with c_right:
            # Action
            st.markdown("### Lựa chọn")
            if st.checkbox("Accept Seed này", value=is_accepted, key=f"chk_{cid}"):
                st.session_state.decisions[cid] = True
            else:
                st.session_state.decisions[cid] = False
                
st.markdown("---")
if st.button("Lưu dữ liệu", key="btn_bottom"):
    whitelisted = [r for r in records if st.session_state.decisions.get(r["chunk_id"])]
    save_data(whitelisted)
    st.success(f"Đã lưu {len(whitelisted)} chunks vào output!")
