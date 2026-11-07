"""Shared immutable source-reference contract for the v2 header migration."""

SOURCE_REFERENCE_COLUMNS = {
    "market.fact_hot_payloads": "id",
    "market.fact_archive_material_aliases": "fact_version_id",
    "market.fact_archive_canonical_dependencies": "fact_version_id",
    "market.fact_archive_prepared_material_aliases": "fact_version_id",
    "market.fact_archive_prepared_canonical_dependencies": "fact_version_id",
}
