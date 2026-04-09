import sys, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from etl.vn import vn_txt_to_jsonl

def run():
    try:
        summary = vn_txt_to_jsonl.process_directory(
            source_dir="../../rag-data/data_intermediate/vmj_ojs_split_articles",
            output_path="../../data/data_final/vmj_ojs.jsonl",
            source_id="vmj_ojs"
        )
        print("SUCCESS")
        print(summary)
    except Exception as e:
        print("ERROR DUMP:")
        traceback.print_exc()

if __name__ == "__main__":
    run()
