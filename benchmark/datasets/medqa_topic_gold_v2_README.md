# MedQA Topic Gold v2

This benchmark targets realistic user questions for the RAG chatbot. It is separate from the VMJ article-specific gold set.

Use this dataset when evaluating:

- Topic/professional medical questions.
- Open-enriched answers with RAG evidence plus safe LLM background explanation.
- Retrieval quality by topic coverage, not by guessing a single article title.
- Answer length, must-have concept coverage, and safety boundaries.

Do not use Top1 title hit as the main metric for this dataset. These questions intentionally do not say "theo nghiên cứu này" or provide an article title.

## Files

- `benchmark/datasets/medqa_topic_gold_v2.jsonl`: 24 records split into `dev`, `test`, and `holdout`.
- `benchmark/runners/run_topic_gold_eval.py`: calls `/api/chat` and records routing, retrieval, answer length, and topic-source hits.
- `benchmark/runners/score_topic_gold.py`: semantic-lite scorer tuned for topic/professional UX.

## Run

```powershell
python benchmark\runners\run_topic_gold_eval.py `
  --dataset-file benchmark\datasets\medqa_topic_gold_v2.jsonl `
  --split all `
  --raw-output benchmark\datasets\test_results\medqa_topic_gold_v2_raw.jsonl `
  --summary-output benchmark\datasets\test_results\medqa_topic_gold_v2_summary.md

python benchmark\runners\score_topic_gold.py `
  --raw-file benchmark\datasets\test_results\medqa_topic_gold_v2_raw.jsonl `
  --detail-output benchmark\datasets\test_results\medqa_topic_gold_v2_details.jsonl `
  --summary-output benchmark\datasets\test_results\medqa_topic_gold_v2_semantic_summary.md
```

Expected direction after the router/retriever update:

- `open_enriched_rate` should be high for this dataset.
- `article_centric_rate` should be near zero.
- `avg_topic_source_hit_rate` should be used to debug retrieval.
- `avg_answer_words` and `under_min_length_rate` should be used to debug answer richness.
