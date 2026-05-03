"""
Generate 75+ eval queries aligned to the EN dataset release in rag-data/datasets/.
Each query is tagged with category for per-category analysis.

Usage:
  cd services/rag-orchestrator
  python eval_queries_gen.py
"""

import json

eval_queries = [
    # ═══════════════════════════════════════════════════════════════════
    #  FACT / GUIDELINE QUERIES (~30)
    #  Kiểm tra: top 3 có trả đúng article không
    # ═══════════════════════════════════════════════════════════════════

    # MedlinePlus
    {"query": "What is an A1C test and how is it used for diabetes?", "expected_source": "MedlinePlus", "expected_title": "A1C", "category": "fact"},
    {"query": "What causes abdominal pain?", "expected_source": "MedlinePlus", "expected_title": "Abdominal Pain", "category": "fact"},
    {"query": "How is acne treated?", "expected_source": "MedlinePlus", "expected_title": "Acne", "category": "fact"},
    {"query": "What is acute bronchitis and how long does it last?", "expected_source": "MedlinePlus", "expected_title": "Acute Bronchitis", "category": "fact"},
    {"query": "What is Addison Disease?", "expected_source": "MedlinePlus", "expected_title": "Addison Disease", "category": "fact"},
    {"query": "What are the symptoms of an abscess?", "expected_source": "MedlinePlus", "expected_title": "Abscess", "category": "fact"},
    {"query": "What causes sickle cell anemia?", "expected_source": "MedlinePlus", "expected_title": "Sickle Cell Disease", "category": "fact"},
    {"query": "How is glaucoma diagnosed?", "expected_source": "MedlinePlus", "expected_title": "Glaucoma", "category": "fact"},
    {"query": "What are the symptoms of acoustic neuroma?", "expected_source": "MedlinePlus", "expected_title": "Acoustic Neuroma", "category": "fact"},
    {"query": "What is acupuncture used for?", "expected_source": "MedlinePlus", "expected_title": "Acupuncture", "category": "fact"},
    {"query": "What are the types of adrenal gland disorders?", "expected_source": "MedlinePlus", "expected_title": "Adrenal Gland Disorders", "category": "fact"},

    # WHO
    {"query": "What are the health effects of ambient air pollution?", "expected_source": "WHO", "expected_title": "Ambient (outdoor) air pollution", "category": "fact"},
    {"query": "What is antimicrobial resistance and why is it a global threat?", "expected_source": "WHO", "expected_title": "Antimicrobial resistance", "category": "fact"},
    {"query": "How should animal bites be treated?", "expected_source": "WHO", "expected_title": "Animal bites", "category": "fact"},
    {"query": "What is the global burden of cardiovascular diseases?", "expected_source": "WHO", "expected_title": "Cardiovascular diseases (CVDs)", "category": "fact"},
    {"query": "What causes cholera and how is it transmitted?", "expected_source": "WHO", "expected_title": "Cholera", "category": "fact"},
    {"query": "What is cervical cancer and how can it be prevented?", "expected_source": "WHO", "expected_title": "Cervical cancer", "category": "fact"},
    {"query": "What are the health consequences of burns?", "expected_source": "WHO", "expected_title": "Burns", "category": "fact"},
    {"query": "What is Chagas disease?", "expected_source": "WHO", "expected_title": "Chagas disease (also known as American trypanosomiasis)", "category": "fact"},
    {"query": "How does arsenic contamination affect health?", "expected_source": "WHO", "expected_title": "Arsenic", "category": "fact"},
    {"query": "What support does assistive technology provide?", "expected_source": "WHO", "expected_title": "Assistive technology", "category": "fact"},

    # NCBI Bookshelf
    {"query": "What is renal denervation therapy for hypertension?", "expected_source": "NCBI Bookshelf", "expected_title": "Renal Denervation Therapy for Drug-Resistant Hypertension", "category": "fact"},
    {"query": "What is the pathophysiology of heart failure?", "expected_source": "NCBI Bookshelf", "category": "fact"},
    {"query": "What are the indications for renal denervation?", "expected_source": "NCBI Bookshelf", "expected_title": "Renal Denervation Therapy for Drug-Resistant Hypertension", "category": "fact"},
    {"query": "What equipment is needed for renal denervation procedures?", "expected_source": "NCBI Bookshelf", "expected_title": "Renal Denervation Therapy for Drug-Resistant Hypertension", "category": "fact"},

    # ═══════════════════════════════════════════════════════════════════
    #  TRUNCATED LIST / FORMATTING QUERIES (~15)
    #  Kiểm tra: chunk truncated list có làm answer bị thiếu ý không
    # ═══════════════════════════════════════════════════════════════════
    {"query": "What are the forms of child maltreatment?", "expected_source": "WHO", "expected_title": "Child maltreatment", "category": "truncated_list"},
    {"query": "List the risk factors for breast cancer.", "expected_source": "WHO", "expected_title": "Breast cancer", "category": "truncated_list"},
    {"query": "What are the symptoms of Chagas disease in the acute phase?", "expected_source": "WHO", "expected_title": "Chagas disease (also known as American trypanosomiasis)", "category": "truncated_list"},
    {"query": "What are the complications of cholera?", "expected_source": "WHO", "expected_title": "Cholera", "category": "truncated_list"},
    {"query": "List the signs of childhood cancer.", "expected_source": "WHO", "expected_title": "Childhood cancer", "category": "truncated_list"},
    {"query": "What tests are used to check for glaucoma?", "expected_source": "MedlinePlus", "expected_title": "Glaucoma", "category": "truncated_list"},
    {"query": "What are the types of acute leukemia?", "expected_source": "MedlinePlus", "expected_title": "Acute Lymphocytic Leukemia", "category": "truncated_list"},
    {"query": "List the symptoms of Addison Disease.", "expected_source": "MedlinePlus", "expected_title": "Addison Disease", "category": "truncated_list"},
    {"query": "What are the types of adrenal gland cancer?", "expected_source": "MedlinePlus", "expected_title": "Adrenal Gland Cancer", "category": "truncated_list"},
    {"query": "What are the contraindications for renal denervation?", "expected_source": "NCBI Bookshelf", "expected_title": "Renal Denervation Therapy for Drug-Resistant Hypertension", "category": "truncated_list"},
    {"query": "What are the common side effects of chemotherapy?", "expected_source": "MedlinePlus", "category": "truncated_list"},
    {"query": "Enumerate the causes of abdominal pain.", "expected_source": "MedlinePlus", "expected_title": "Abdominal Pain", "category": "truncated_list"},
    {"query": "What are the prevention strategies for cardiovascular diseases?", "expected_source": "WHO", "expected_title": "Cardiovascular diseases (CVDs)", "category": "truncated_list"},

    # ═══════════════════════════════════════════════════════════════════
    #  MIXED TOPIC / CONFUSION QUERIES (~12)
    #  Kiểm tra: bài mixed-topic có gây retrieve sai không
    # ═══════════════════════════════════════════════════════════════════
    {"query": "What is asthma?", "expected_source": "WHO", "expected_title": "Asthma", "category": "mixed_topic"},
    {"query": "What causes anxiety disorders?", "expected_source": "WHO", "expected_title": "Anxiety disorders", "category": "mixed_topic"},
    {"query": "How is autism diagnosed?", "expected_source": "WHO", "expected_title": "Autism", "category": "mixed_topic"},
    {"query": "What is bipolar disorder?", "expected_source": "WHO", "expected_title": "Bipolar disorder", "category": "mixed_topic"},
    {"query": "What to do if someone has a seizure?", "expected_source": "MedlinePlus", "category": "mixed_topic"},
    {"query": "How is alcohol use disorder treated?", "expected_source": "MedlinePlus", "expected_title": "Alcohol Use Disorder (AUD) Treatment", "category": "mixed_topic"},
    {"query": "What are the health effects of alcohol?", "expected_source": "WHO", "expected_title": "Alcohol", "category": "mixed_topic"},
    {"query": "What is the difference between type 1 and type 2 diabetes?", "expected_source": "MedlinePlus", "category": "mixed_topic"},
    {"query": "What causes cancer?", "expected_source": "WHO", "expected_title": "Cancer", "category": "mixed_topic"},
    {"query": "Is blindness preventable?", "expected_source": "WHO", "expected_title": "Blindness and vision impairment", "category": "mixed_topic"},
    {"query": "How does ageing affect health?", "expected_source": "WHO", "expected_title": "Ageing and health", "category": "mixed_topic"},
    {"query": "What is adolescent pregnancy?", "expected_source": "WHO", "expected_title": "Adolescent pregnancy", "category": "mixed_topic"},

    # ═══════════════════════════════════════════════════════════════════
    #  FILTER EFFECTIVENESS QUERIES (~10)
    #  Kiểm tra: audience/source filter hoạt động đúng không
    # ═══════════════════════════════════════════════════════════════════
    {"query": "Patient friendly explanation of diabetes type 2", "expected_source": "MedlinePlus", "category": "filter"},
    {"query": "Clinical guidelines for managing hypertension", "expected_source": "NCBI Bookshelf", "category": "filter"},
    {"query": "Global statistics on maternal mortality", "expected_source": "WHO", "category": "filter"},
    {"query": "Explain acne treatment in simple terms for patients", "expected_source": "MedlinePlus", "expected_title": "Acne", "category": "filter"},
    {"query": "WHO recommendation on antimicrobial resistance", "expected_source": "WHO", "expected_title": "Antimicrobial resistance", "category": "filter"},
    {"query": "Pathophysiology and mechanism of renal denervation", "expected_source": "NCBI Bookshelf", "category": "filter"},
    {"query": "Easy to understand information about acute bronchitis", "expected_source": "MedlinePlus", "expected_title": "Acute Bronchitis", "category": "filter"},
    {"query": "Evidence-based guidelines for cholera treatment", "expected_source": "WHO", "expected_title": "Cholera", "category": "filter"},
    {"query": "Textbook explanation of cardiovascular disease mechanisms", "expected_source": "NCBI Bookshelf", "category": "filter"},
    {"query": "WHO guidelines on adolescent health", "expected_source": "WHO", "expected_title": "Adolescent and young adult health", "category": "filter"},

    # ═══════════════════════════════════════════════════════════════════
    #  MULTI-TURN / AMBIGUOUS QUERIES (~8)
    #  Kiểm tra: query rewrite & ambiguous handling
    # ═══════════════════════════════════════════════════════════════════
    {"query": "What about the treatment for it?", "expected_source": "NCBI Bookshelf", "category": "multi_turn"},
    {"query": "Can it be cured?", "expected_source": "MedlinePlus", "category": "multi_turn"},
    {"query": "Who is most at risk?", "expected_source": "WHO", "category": "multi_turn"},
    {"query": "What are the side effects?", "expected_source": "MedlinePlus", "category": "multi_turn"},
    {"query": "Tell me more about the diagnosis.", "expected_source": "NCBI Bookshelf", "category": "multi_turn"},
    {"query": "How is it prevented?", "expected_source": "WHO", "category": "multi_turn"},
    {"query": "Is there a vaccine?", "expected_source": "WHO", "category": "multi_turn"},
    {"query": "What about children?", "expected_source": "MedlinePlus", "category": "multi_turn"},
]

# Write
with open("eval_queries.json", "w", encoding="utf-8") as f:
    json.dump(eval_queries, f, indent=2, ensure_ascii=False)

# Summary
from collections import Counter
cats = Counter(q["category"] for q in eval_queries)
srcs = Counter(q["expected_source"] for q in eval_queries)

print(f"Created eval_queries.json with {len(eval_queries)} test queries.")
print(f"\nBy category:")
for cat, cnt in cats.most_common():
    print(f"  {cat}: {cnt}")
print(f"\nBy expected source:")
for src, cnt in srcs.most_common():
    print(f"  {src}: {cnt}")
