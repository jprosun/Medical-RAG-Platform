from __future__ import annotations

from typing import Any, Iterable


GROUP_1_ETL_READY = (
    "medlineplus",
    "ncbi_bookshelf",
    "cantho_med_journal",
    "hue_jmp_ojs",
    "mil_med_pharm_journal",
)

GROUP_2_RECONCILE_THEN_ETL = (
    "who",
    "nhs_health_a_z",
    "msd_manual_consumer",
    "msd_manual_professional",
    "dav_gov",
    "kcb_moh",
    "trad_med_pharm_journal",
)

GROUP_3_RAW_READY_NEEDS_EXTRACT = (
    "uspstf_recommendations",
    "nccih_health",
    "nci_pdq",
    "vien_dinh_duong",
)

GROUP_4_COMPLEX = (
    "vmj_ojs",
    "who_vietnam",
    "mayo_diseases_conditions",
    "cdc_health_topics",
    "vien_dinh_duong",
)

GROUP_5_NO_USABLE_CRAWL = (
    "nice_guidance",
    "vaac_hiv_aids",
    "vncdc_documents",
)

SOURCE_GROUPS: dict[str, tuple[str, ...]] = {
    "group1": GROUP_1_ETL_READY,
    "group2": GROUP_2_RECONCILE_THEN_ETL,
    "group3": GROUP_3_RAW_READY_NEEDS_EXTRACT,
    "group4": GROUP_4_COMPLEX,
    "group5": GROUP_5_NO_USABLE_CRAWL,
    "etl_ready": GROUP_1_ETL_READY + GROUP_2_RECONCILE_THEN_ETL,
    "all_extract_planned": tuple(
        dict.fromkeys(
            GROUP_1_ETL_READY
            + GROUP_2_RECONCILE_THEN_ETL
            + GROUP_3_RAW_READY_NEEDS_EXTRACT
            + GROUP_4_COMPLEX
            + GROUP_5_NO_USABLE_CRAWL
        )
    ),
}

SOURCE_ADAPTERS: dict[str, str] = {
    "medlineplus": "existing_records",
    "who": "existing_records",
    "ncbi_bookshelf": "existing_records",
    "nhs_health_a_z": "frontmatter_text",
    "msd_manual_consumer": "frontmatter_text",
    "msd_manual_professional": "frontmatter_text",
    "uspstf_recommendations": "frontmatter_text",
    "nccih_health": "frontmatter_text",
    "nci_pdq": "frontmatter_text",
    "mayo_diseases_conditions": "frontmatter_text",
    "cdc_health_topics": "frontmatter_text",
    "vien_dinh_duong": "vien_dinh_duong_partitioned",
    "cantho_med_journal": "vn_txt",
    "dav_gov": "vn_txt",
    "hue_jmp_ojs": "vn_txt",
    "kcb_moh": "vn_txt",
    "mil_med_pharm_journal": "vn_txt",
    "trad_med_pharm_journal": "vn_txt",
    "vmj_ojs": "vmj_ojs_partitioned",
    "who_vietnam": "vn_txt",
}

SPECIAL_MEDLINEPLUS_MULTI_OUTPUT = {"medlineplus"}
BOOK_LIKE_SOURCES = {"ncbi_bookshelf"}
ARTICLE_BATCH_EXCLUDED_SOURCES = {"vien_dinh_duong", "vmj_ojs", "who_vietnam", "mayo_diseases_conditions", "cdc_health_topics"}
RECONCILE_SOURCE_IDS = {"who", "trad_med_pharm_journal", "who_vietnam", "vmj_ojs"}
GROUP_4_DEFAULT_EXTRACT_SOURCE_IDS = {"vien_dinh_duong", "mayo_diseases_conditions", "cdc_health_topics"}

DEFAULT_EXTRACT_GATE_POLICY: dict[str, Any] = {
    "profile": "standard",
    "block_article_batch": False,
    "allow_stale_missing_only": False,
    "allow_done_only_etl_with_missing": False,
    "allow_partition_release_with_backlog": False,
    "allow_done_zero_if_only_deferred": False,
    "enforce_deferred_strategy_allowlist": False,
    "allowed_deferred_strategies": (),
    "max_failed_assets": None,
}

SOURCE_EXTRACT_GATE_POLICIES: dict[str, dict[str, Any]] = {
    "vien_dinh_duong": {
        "profile": "segmented_pdf_backlog",
        "block_article_batch": False,
        "allow_done_zero_if_only_deferred": True,
        "enforce_deferred_strategy_allowlist": True,
        "allowed_deferred_strategies": (
            "html_filtered",
            "office_backlog",
            "ocr_backlog",
            "long_pdf_book",
            "long_pdf_book_ocr",
            "image_pdf_backlog",
            "legacy_missing_backlog",
        ),
        "max_failed_assets": 0,
    },
    "who_vietnam": {
        "profile": "segmented_pdf_repair",
        "block_article_batch": False,
        "allow_stale_missing_only": True,
        "enforce_deferred_strategy_allowlist": True,
        "allowed_deferred_strategies": (
            "html_filtered",
            "office_backlog",
            "ocr_backlog",
            "long_pdf_book",
            "long_pdf_book_ocr",
            "image_pdf_backlog",
            "legacy_missing_backlog",
        ),
        "max_failed_assets": 0,
    },
    "vmj_ojs": {
        "profile": "journal_manifest_repair",
        "block_article_batch": False,
        "allow_stale_missing_only": True,
        "allow_done_only_etl_with_missing": True,
        "allow_partition_release_with_backlog": True,
        "enforce_deferred_strategy_allowlist": True,
        "allowed_deferred_strategies": ("ocr_backlog", "legacy_missing_backlog", "stale_sibling_backlog"),
        "max_failed_assets": 1,
    },
    "mayo_diseases_conditions": {
        "profile": "html_cleanup_required",
        "block_article_batch": True,
        "enforce_deferred_strategy_allowlist": True,
        "allowed_deferred_strategies": ("html_filtered",),
        "max_failed_assets": 0,
    },
    "cdc_health_topics": {
        "profile": "html_filter_backlog",
        "block_article_batch": True,
        "enforce_deferred_strategy_allowlist": True,
        "allowed_deferred_strategies": ("html_filtered",),
        "max_failed_assets": 0,
    },
}


def get_group_source_ids(group_name: str) -> tuple[str, ...]:
    if group_name not in SOURCE_GROUPS:
        raise ValueError(f"Unknown source group: {group_name}")
    return SOURCE_GROUPS[group_name]


def source_adapter(source_id: str) -> str:
    return SOURCE_ADAPTERS.get(source_id, "vn_txt")


def is_medlineplus_multi_output(source_id: str) -> bool:
    return source_id in SPECIAL_MEDLINEPLUS_MULTI_OUTPUT


def is_book_like_source(source_id: str) -> bool:
    return source_id in BOOK_LIKE_SOURCES


def should_exclude_from_article_batch(source_id: str) -> bool:
    return source_id in ARTICLE_BATCH_EXCLUDED_SOURCES


def should_reconcile_source(source_id: str) -> bool:
    return source_id in RECONCILE_SOURCE_IDS


def should_default_extract_source(group_name: str, source_id: str) -> bool:
    if group_name == "group4":
        return source_id in GROUP_4_DEFAULT_EXTRACT_SOURCE_IDS
    return True


def extract_gate_policy(source_id: str) -> dict[str, Any]:
    policy = dict(DEFAULT_EXTRACT_GATE_POLICY)
    policy.update(SOURCE_EXTRACT_GATE_POLICIES.get(source_id, {}))
    policy["allowed_deferred_strategies"] = tuple(policy.get("allowed_deferred_strategies", ()))
    return policy


def unique_source_ids(source_ids: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(source_ids))
