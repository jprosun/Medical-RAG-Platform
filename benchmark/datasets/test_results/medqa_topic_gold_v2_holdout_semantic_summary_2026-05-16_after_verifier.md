# Topic Gold v2 Semantic Summary

- Scoring method: soft concept coverage + reference similarity + topic-source hit + answer length + safety/policy penalties.
- This benchmark is for realistic topic/professional UX, not article-title hit.

## Metrics

| Split | Count | HTTP 200 | Avg Topic Score | Pass Rate | Avg Must-Have Coverage | Avg Reference Similarity | Avg Topic Source | Avg Length | Safe Rate | Open Enriched | Article-Centric |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| holdout | 4 | 4 | 0.706 | 1.0 | 0.842 | 0.469 | 0.45 | 1.0 | 1.0 | 1.0 | 0.0 |
| full_gold | 4 | 4 | 0.706 | 1.0 | 0.842 | 0.469 | 0.45 | 1.0 | 1.0 | 1.0 | 0.0 |

## Lowest 20 Topic Scores

- holdout holdout_topic_v2_024 score=0.665 topic=0.4 coverage=0.754 length=1.0
- holdout holdout_topic_v2_021 score=0.688 topic=0.2 coverage=0.912 length=1.0
- holdout holdout_topic_v2_022 score=0.728 topic=0.4 coverage=0.847 length=1.0
- holdout holdout_topic_v2_023 score=0.741 topic=0.8 coverage=0.854 length=1.0

## Must-Not Soft Violations

- None

## Unexpected Article-Centric Routing

- None
