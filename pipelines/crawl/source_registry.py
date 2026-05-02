from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    mode: str
    display_name: str
    wave: int
    item_type: str = ""
    content_class: str = ""


SOURCE_REGISTRY: dict[str, SourceConfig] = {
    "medlineplus": SourceConfig(
        source_id="medlineplus",
        mode="medlineplus_xml",
        display_name="MedlinePlus",
        wave=1,
        item_type="xml_feed",
        content_class="xml",
    ),
    "who": SourceConfig(
        source_id="who",
        mode="who_expanded",
        display_name="World Health Organization",
        wave=1,
        item_type="fact_sheet",
        content_class="html",
    ),
    "nhs_health_a_z": SourceConfig(
        source_id="nhs_health_a_z",
        mode="basic_topic_site",
        display_name="NHS Health A-Z",
        wave=1,
        item_type="health_topic",
        content_class="html",
    ),
    "msd_manual_consumer": SourceConfig(
        source_id="msd_manual_consumer",
        mode="basic_topic_site",
        display_name="MSD Manual Consumer",
        wave=1,
        item_type="consumer_topic",
        content_class="html",
    ),
    "ncbi_bookshelf": SourceConfig(
        source_id="ncbi_bookshelf",
        mode="ncbi_bookshelf",
        display_name="NCBI Bookshelf",
        wave=3,
        item_type="book_chapter",
        content_class="html_book",
    ),
    "msd_manual_professional": SourceConfig(
        source_id="msd_manual_professional",
        mode="basic_topic_site",
        display_name="MSD Manual Professional",
        wave=2,
        item_type="professional_topic",
        content_class="html",
    ),
    "cdc_health_topics": SourceConfig(
        source_id="cdc_health_topics",
        mode="basic_topic_site",
        display_name="CDC Health Topics",
        wave=2,
        item_type="health_topic",
        content_class="html",
    ),
    "mayo_diseases_conditions": SourceConfig(
        source_id="mayo_diseases_conditions",
        mode="basic_topic_site",
        display_name="Mayo Clinic Diseases and Conditions",
        wave=2,
        item_type="disease_condition",
        content_class="html",
    ),
    "nice_guidance": SourceConfig(
        source_id="nice_guidance",
        mode="reference_site",
        display_name="NICE Guidance",
        wave=2,
        item_type="guidance_page",
        content_class="html",
    ),
    "uspstf_recommendations": SourceConfig(
        source_id="uspstf_recommendations",
        mode="reference_site",
        display_name="USPSTF Recommendations",
        wave=2,
        item_type="recommendation_page",
        content_class="html",
    ),
    "nccih_health": SourceConfig(
        source_id="nccih_health",
        mode="reference_site",
        display_name="NCCIH Health Topics",
        wave=2,
        item_type="health_topic",
        content_class="html",
    ),
    "nci_pdq": SourceConfig(
        source_id="nci_pdq",
        mode="reference_site",
        display_name="NCI PDQ",
        wave=2,
        item_type="pdq_summary",
        content_class="html",
    ),
    "vncdc_documents": SourceConfig(
        source_id="vncdc_documents",
        mode="reference_site",
        display_name="VNCDC Documents",
        wave=2,
        item_type="document_page",
    ),
    "vaac_hiv_aids": SourceConfig(
        source_id="vaac_hiv_aids",
        mode="reference_site",
        display_name="VAAC HIV/AIDS",
        wave=2,
        item_type="hiv_guidance_page",
    ),
    "vien_dinh_duong": SourceConfig(
        source_id="vien_dinh_duong",
        mode="reference_site",
        display_name="Viện Dinh Dưỡng",
        wave=2,
        item_type="nutrition_page",
    ),
    "who_vietnam": SourceConfig(
        source_id="who_vietnam",
        mode="who_vietnam_site",
        display_name="WHO Vietnam",
        wave=3,
    ),
    "kcb_moh": SourceConfig(
        source_id="kcb_moh",
        mode="seed_catalog_refresh",
        display_name="Ministry of Health - KCB",
        wave=2,
    ),
    "dav_gov": SourceConfig(
        source_id="dav_gov",
        mode="seed_catalog_refresh",
        display_name="Drug Administration of Vietnam",
        wave=2,
    ),
    "hue_jmp_ojs": SourceConfig(
        source_id="hue_jmp_ojs",
        mode="seed_catalog_refresh",
        display_name="Hue Journal of Medicine and Pharmacy",
        wave=3,
    ),
    "cantho_med_journal": SourceConfig(
        source_id="cantho_med_journal",
        mode="seed_catalog_refresh",
        display_name="Can Tho Medical Journal",
        wave=4,
    ),
    "mil_med_pharm_journal": SourceConfig(
        source_id="mil_med_pharm_journal",
        mode="seed_catalog_refresh",
        display_name="Military Medical and Pharmacy Journal",
        wave=3,
    ),
    "trad_med_pharm_journal": SourceConfig(
        source_id="trad_med_pharm_journal",
        mode="seed_catalog_refresh",
        display_name="Traditional Medicine and Pharmacy Journal",
        wave=3,
    ),
    "vmj_ojs": SourceConfig(
        source_id="vmj_ojs",
        mode="vmj_ojs_site",
        display_name="Vietnam Medical Journal",
        wave=4,
    ),
}


def sources_for_wave(wave: int) -> list[SourceConfig]:
    return [config for config in SOURCE_REGISTRY.values() if config.wave == wave]


def source_ids_for_wave(wave: int) -> list[str]:
    return [config.source_id for config in sources_for_wave(wave)]
