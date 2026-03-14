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


class SchemaAnalyzer:
    """Introspect a Solr schema and select fields suitable for data generation."""

    def __init__(self, solr_client):
        self.client = solr_client

    def analyze(self, max_fields: int = 20, fields_per_type: int = 3) -> list[FieldSpec]:
        """Return a list of FieldSpecs to populate, ensuring type diversity."""
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

        # Ensure unique-key field is always included
        selected = self._select_diverse(specs, max_fields, fields_per_type)
        return selected

    @staticmethod
    def _select_diverse(
        specs: list[FieldSpec], max_fields: int, fields_per_type: int
    ) -> list[FieldSpec]:
        """Pick up to `fields_per_type` fields per category, capped at `max_fields`."""
        # Always include unique-key and required fields first
        must_have = [s for s in specs if s.is_unique_key or s.required]
        remaining = [s for s in specs if not s.is_unique_key and not s.required]

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
