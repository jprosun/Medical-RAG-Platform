# Semantic-Lite Summary

- Scoring method: soft concept match + short_answer/ground_truth similarity + retrieval hit + boundary penalties
- This is stricter than raw substring match, but still lighter than an LLM judge.

## Metrics

| Split | Count | HTTP 200 | Avg Semantic-Lite Score | Semantic-Lite Pass Rate | Avg Must-Have Soft Coverage | Avg Reference Similarity | Safe Rate | False Insufficiency Rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| dev | 64 | 64 | 0.612 | 0.516 | 0.709 | 0.457 | 0.969 | 0.078 |
| test | 20 | 20 | 0.608 | 0.5 | 0.716 | 0.496 | 1.0 | 0.1 |
| holdout | 18 | 18 | 0.656 | 0.611 | 0.742 | 0.446 | 1.0 | 0.167 |
| full_gold | 102 | 102 | 0.619 | 0.529 | 0.716 | 0.463 | 0.98 | 0.098 |

## Lowest 20 Semantic-Lite Scores

- dev q_034 score=0.272 ref_sim=0.534 coverage=0.41
- dev q_050 score=0.285 ref_sim=0.357 coverage=0.355
- dev q_062 score=0.301 ref_sim=0.432 coverage=1.0
- dev q_071 score=0.316 ref_sim=0.256 coverage=0.502
- dev q_026 score=0.317 ref_sim=0.421 coverage=0.376
- test q_060 score=0.336 ref_sim=0.317 coverage=0.5
- dev q_087 score=0.349 ref_sim=0.399 coverage=0.466
- dev q_052 score=0.36 ref_sim=0.317 coverage=0.554
- test q_033 score=0.381 ref_sim=0.568 coverage=0.404
- dev q_078 score=0.391 ref_sim=0.45 coverage=0.518
- dev q_099 score=0.398 ref_sim=0.347 coverage=0.615
- test q_004 score=0.408 ref_sim=0.308 coverage=0.667
- dev q_039 score=0.41 ref_sim=0.406 coverage=0.594
- dev q_059 score=0.428 ref_sim=0.35 coverage=0.679
- dev q_066 score=0.437 ref_sim=0.518 coverage=0.569
- holdout q_072 score=0.441 ref_sim=0.413 coverage=0.66
- test q_022 score=0.455 ref_sim=0.442 coverage=0.667
- dev q_053 score=0.473 ref_sim=0.275 coverage=0.392
- dev q_025 score=0.475 ref_sim=0.432 coverage=0.72
- holdout q_042 score=0.488 ref_sim=0.445 coverage=0.559

## False Insufficiency Flags

- dev q_003 score=0.738
- dev q_023 score=0.59
- dev q_044 score=0.757
- dev q_045 score=0.537
- dev q_076 score=0.724
- test q_036 score=0.73
- test q_064 score=0.62
- holdout q_012 score=0.505
- holdout q_042 score=0.488
- holdout q_068 score=0.594

## Must-Not Soft Violations

- dev q_034 hits=Ăn cơm làm tăng nguy cơ suy dinh dưỡng
- dev q_062 hits=TNF-α là một interleukin
