# Semantic-Lite Summary

- Scoring method: soft concept match + short_answer/ground_truth similarity + retrieval hit + boundary penalties
- This is stricter than raw substring match, but still lighter than an LLM judge.

## Metrics

| Split | Count | HTTP 200 | Avg Semantic-Lite Score | Semantic-Lite Pass Rate | Avg Must-Have Soft Coverage | Avg Reference Similarity | Safe Rate | False Insufficiency Rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| dev | 64 | 64 | 0.59 | 0.484 | 0.732 | 0.453 | 0.922 | 0.031 |
| test | 20 | 20 | 0.668 | 0.65 | 0.815 | 0.473 | 1.0 | 0.15 |
| holdout | 18 | 18 | 0.734 | 0.833 | 0.845 | 0.536 | 1.0 | 0.167 |
| full_gold | 102 | 102 | 0.631 | 0.578 | 0.768 | 0.472 | 0.951 | 0.078 |

## Lowest 20 Semantic-Lite Scores

- dev q_062 score=0.195 ref_sim=0.385 coverage=0.134
- dev q_025 score=0.196 ref_sim=0.453 coverage=0.75
- dev q_008 score=0.28 ref_sim=0.302 coverage=0.388
- dev q_026 score=0.319 ref_sim=0.484 coverage=0.333
- dev q_009 score=0.322 ref_sim=0.4 coverage=0.405
- dev q_081 score=0.325 ref_sim=0.439 coverage=0.381
- dev q_035 score=0.337 ref_sim=0.375 coverage=0.457
- dev q_087 score=0.341 ref_sim=0.234 coverage=0.398
- dev q_050 score=0.348 ref_sim=0.351 coverage=0.501
- dev q_059 score=0.359 ref_sim=0.372 coverage=0.508
- dev q_071 score=0.359 ref_sim=0.26 coverage=0.595
- dev q_066 score=0.364 ref_sim=0.547 coverage=0.605
- test q_060 score=0.366 ref_sim=0.337 coverage=0.55
- holdout q_007 score=0.384 ref_sim=0.356 coverage=0.577
- test q_022 score=0.387 ref_sim=0.377 coverage=0.567
- dev q_076 score=0.394 ref_sim=0.507 coverage=0.482
- dev q_099 score=0.395 ref_sim=0.333 coverage=0.619
- dev q_052 score=0.431 ref_sim=0.351 coverage=0.507
- holdout q_072 score=0.45 ref_sim=0.429 coverage=0.667
- dev q_094 score=0.482 ref_sim=0.345 coverage=0.802

## False Insufficiency Flags

- dev q_019 score=0.603
- dev q_100 score=0.656
- test q_058 score=0.591
- test q_064 score=0.689
- test q_074 score=0.504
- holdout q_068 score=0.579
- holdout q_073 score=0.739
- holdout q_082 score=0.827

## Must-Not Soft Violations

- dev q_005 hits=Kiểu hình CD34(+)/HLA-DR(-) liên quan đáp ứng tốt hơn
- dev q_025 hits=Nghiên cứu nói về phẫu thuật mở thay vì nội soi
- dev q_034 hits=Ăn cơm làm tăng nguy cơ suy dinh dưỡng
- dev q_066 hits=Có bệnh nhân suy giáp lâm sàng trong mẫu nghiên cứu
- dev q_090 hits=Kiến thức chung về lây nhiễm HIV không liên quan đến kiến thức qua QHTD
