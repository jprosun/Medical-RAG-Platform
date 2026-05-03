import sys, traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from pipelines.etl.vn import vn_txt_to_jsonl
from services.utils.data_paths import preferred_intermediate_dir, source_records_path

def run():
    try:
        summary = vn_txt_to_jsonl.process_directory(
            source_dir=str(preferred_intermediate_dir("vmj_ojs", "split_articles")),
            output_path=str(source_records_path("vmj_ojs")),
            source_id="vmj_ojs"
        )
        print("SUCCESS")
        print(summary)
    except Exception as e:
        print("ERROR DUMP:")
        traceback.print_exc()

if __name__ == "__main__":
    run()
