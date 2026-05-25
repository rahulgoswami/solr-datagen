"""Schema introspection — fetch fields, classify types, select a diverse subset."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from solr_datagen.config import FIELD_TYPE_MAP, INTERNAL_FIELDS

logger = logging.getLogger(__name__)


@dataclass
class FieldSpec:
    name: str
    category: str
    multi_valued: bool
    is_unique_key: bool
    required: bool


@dataclass
class ReindexSchema:
    """Minimal schema info needed to drive a reindex run."""
    unique_key: str
    copy_field_rules: list[dict]        # raw list from /schema/copyfields
    unique_key_field_info: dict         # field definition dict from /schema/fields/<name>
    unique_key_field_type: dict         # field type definition dict matching the field's type


def prepare_reindex_schema(client) -> ReindexSchema:
    """Fetch uniqueKey, copyField rules, and uniqueKey type info for reindex pre-checks.

    Does NOT touch field selection, FIELD_TYPE_MAP, or category logic — those belong
    to index mode only.
    """
    unique_key = client.get_unique_key()
    field_info = client.get_field_info(unique_key)
    field_types = client.get_field_types()
    copy_field_rules = client.get_copy_fields()

    type_name = field_info.get("type", "")
    uk_field_type = next((ft for ft in field_types if ft["name"] == type_name), {})

    return ReindexSchema(
        unique_key=unique_key,
        copy_field_rules=copy_field_rules,
        unique_key_field_info=field_info,
        unique_key_field_type=uk_field_type,
    )


class SchemaAnalyzer:
    """Introspect a Solr schema and select fields suitable for data generation."""

    def __init__(self, solr_client):
        self.client = solr_client

    def analyze(
        self,
        max_fields: int = 20,
        fields_per_type: int = 3,
        include_fields: list[str] | None = None,
    ) -> list[FieldSpec]:
        """Return a list of FieldSpecs to populate, ensuring type diversity.

        Fields listed in ``include_fields`` are forced into the selection
        regardless of the diverse-selection caps. Raises ``ValueError`` if any
        named field is not present in the schema with a generatable type.
        """
        fields = self.client.get_fields()
        field_types = self.client.get_field_types()
        unique_key = self.client.get_unique_key()

        # Build type_name → class_name lookup
        type_class_map: dict[str, str] = {}
        for ft in field_types:
            class_name = ft.get("class", "")
            # Strip leading 'org.apache.solr.schema.' prefix if present
            short = class_name.rsplit(".", 1)[-1] if "." in class_name else class_name
            type_class_map[ft["name"]] = f"solr.{short}"

        specs: list[FieldSpec] = []
        for field in fields:
            name = field["name"]
            if name in INTERNAL_FIELDS:
                continue

            stored = field.get("stored", True)
            doc_values = field.get("docValues", False)
            if not stored and not doc_values:
                logger.debug("Skipping %s (not stored, no docValues)", name)
                continue

            type_name = field.get("type", "")
            class_name = type_class_map.get(type_name)
            if class_name is None:
                logger.warning("Unknown type name '%s' for field '%s', skipping", type_name, name)
                continue

            category = FIELD_TYPE_MAP.get(class_name)
            if category is None:
                logger.warning("Unmapped class '%s' for field '%s', skipping", class_name, name)
                continue

            specs.append(FieldSpec(
                name=name,
                category=category,
                multi_valued=field.get("multiValued", False),
                is_unique_key=(name == unique_key),
                required=field.get("required", False) or (name == unique_key),
            ))

        # Validate --include-fields names before selection.
        if include_fields:
            spec_names = {s.name for s in specs}
            schema_names = {f["name"] for f in fields}
            missing = [n for n in include_fields if n not in spec_names]
            if missing:
                not_in_schema = [n for n in missing if n not in schema_names]
                unusable = [n for n in missing if n in schema_names]
                parts = []
                if not_in_schema:
                    parts.append(f"not in schema: {', '.join(not_in_schema)}")
                if unusable:
                    parts.append(
                        f"present but unusable (internal, not stored/docValues, "
                        f"or unmapped type): {', '.join(unusable)}"
                    )
                raise ValueError("--include-fields " + "; ".join(parts))

        # Ensure unique-key, required, and explicitly-included fields are always included.
        selected = self._select_diverse(specs, max_fields, fields_per_type, include_fields or ())
        return selected

    @staticmethod
    def _select_diverse(
        specs: list[FieldSpec],
        max_fields: int,
        fields_per_type: int,
        include_fields=(),
    ) -> list[FieldSpec]:
        """Pick up to `fields_per_type` fields per category, capped at `max_fields`."""
        # Always include unique-key, required, and explicitly-requested fields first.
        forced = set(include_fields)
        must_have = [s for s in specs if s.is_unique_key or s.required or s.name in forced]
        remaining = [
            s for s in specs
            if not (s.is_unique_key or s.required or s.name in forced)
        ]

        # Group remaining by category
        by_category: dict[str, list[FieldSpec]] = {}
        for s in remaining:
            by_category.setdefault(s.category, []).append(s)

        selected = list(must_have)
        selected_names = {s.name for s in selected}

        for cat in sorted(by_category):
            for s in by_category[cat]:
                if len(selected) >= max_fields:
                    break
                # Count how many of this category are already selected
                cat_count = sum(1 for x in selected if x.category == cat)
                if cat_count >= fields_per_type:
                    break
                if s.name not in selected_names:
                    selected.append(s)
                    selected_names.add(s.name)

        return selected

    @staticmethod
    def print_summary(specs: list[FieldSpec]) -> None:
        """Print a human-readable table of selected fields."""
        print(f"\n{'Field Name':<30} {'Category':<10} {'Multi':<6} {'Key':<4} {'Req':<4}")
        print("-" * 56)
        for s in specs:
            mv = "yes" if s.multi_valued else ""
            uk = "*" if s.is_unique_key else ""
            req = "yes" if s.required else ""
            print(f"{s.name:<30} {s.category:<10} {mv:<6} {uk:<4} {req:<4}")
        print(f"\nTotal fields selected: {len(specs)}")
