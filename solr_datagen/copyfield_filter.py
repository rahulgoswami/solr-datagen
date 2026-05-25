"""Builds a matcher that identifies fields to drop before reindexing."""

from __future__ import annotations

import fnmatch

# Nested-doc marker fields; not copyField destinations but also not repostable.
# Note: _version_ is intentionally NOT in this set — it must round-trip.
_INTERNAL_DROP = frozenset({"_root_", "_nest_path_"})


def build_drop_matcher(copy_field_rules: list[dict]):
    """Return a callable(field_name) -> bool that is True when the field should be dropped.

    Drops copyField destination fields (concrete names and glob patterns) and
    nested-document marker fields. _version_ is preserved for optimistic concurrency.
    """
    dests = {r["dest"] for r in copy_field_rules}
    concrete = frozenset(d for d in dests if "*" not in d and "?" not in d)
    globs = [d for d in dests if d not in concrete]

    def should_drop(field_name: str) -> bool:
        if field_name in _INTERNAL_DROP or field_name in concrete:
            return True
        return any(fnmatch.fnmatchcase(field_name, g) for g in globs)

    return should_drop
