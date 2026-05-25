"""Command-line interface — parse arguments, orchestrate the pipeline."""

from __future__ import annotations

import argparse
import logging
import re
import signal
import sys

from solr_datagen.config import BATCH_SIZE, COMMIT_WITHIN_MS, READ_BATCH_SIZE
from solr_datagen.data_generator import DataGenerator
from solr_datagen.indexer import DocumentIndexer, GeneratedSource
from solr_datagen.progress import ProgressTracker
from solr_datagen.reindex_source import ReindexSource
from solr_datagen.schema_analyzer import SchemaAnalyzer, prepare_reindex_schema
from solr_datagen.solr_client import SolrClient
from solr_datagen.sort_check import is_sortable_for_cursor


def _needs_index_shim(argv: list[str]) -> bool:
    """Return True if argv looks like a legacy pre-subcommand invocation."""
    if not argv or argv[0] in {"index", "reindex", "-h", "--help"}:
        return False
    # If any token is a Solr URL, it's a legacy invocation (e.g. -v http://... or -a u:p http://...)
    return any(re.match(r"^https?://", tok) for tok in argv)


def _build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "-b", "--batch-size", type=int, default=BATCH_SIZE,
        help="Docs per HTTP POST request (default: %(default)s)",
    )
    common.add_argument(
        "-c", "--commit-within", type=int, default=COMMIT_WITHIN_MS,
        help=(
            "commitWithin in ms; 0 (default) disables per-batch commits and relies on "
            "server-side autoCommit — use a positive value if autoCommit is not configured"
        ),
    )
    common.add_argument(
        "-w", "--workers", type=int, default=4,
        help="Parallel submission threads (default: %(default)s)",
    )
    common.add_argument("-a", "--auth", default=None, help="Basic auth as user:password")
    common.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

    p = argparse.ArgumentParser(
        prog="solr-datagen",
        description=(
            "Generate and index documents into Apache Solr (index mode), "
            "or reindex existing documents in-place (reindex mode)."
        ),
    )
    sub = p.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # ---- index subcommand ----
    idx = sub.add_parser(
        "index",
        parents=[common],
        help="Generate and index synthetic documents",
    )
    idx.add_argument("solr_url", help="Solr collection URL, e.g. http://localhost:8983/solr/my_core")
    idx.add_argument("count", type=int, help="Number of documents to generate")
    idx.add_argument(
        "-f", "--max-fields", type=int, default=20,
        help="Max fields to select from schema (default: %(default)s)",
    )
    idx.add_argument(
        "--fields-per-type", type=int, default=3,
        help="Max fields per type category (default: %(default)s)",
    )
    idx.add_argument("-s", "--seed", type=int, default=None, help="Random seed for reproducibility")
    idx.add_argument(
        "--include-fields", default=None,
        help="Comma-separated field names to always include in every document",
    )
    idx.add_argument("--dry-run", action="store_true", help="Analyse schema only, don't index")

    # ---- reindex subcommand ----
    ridx = sub.add_parser(
        "reindex",
        parents=[common],
        help="Read existing documents from the index and reindex them in-place",
    )
    ridx.add_argument("solr_url", help="Solr collection URL, e.g. http://localhost:8983/solr/my_core")
    ridx.add_argument(
        "--fq", default=None,
        help=(
            "Filter query to limit which documents are reindexed (Solr syntax). "
            "Omit to reindex all documents."
        ),
    )
    ridx.add_argument(
        "--read-batch-size", type=int, default=READ_BATCH_SIZE,
        help="Documents per cursorMark read page (default: %(default)s)",
    )

    return p


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)

    # Back-compat: legacy form `solr-datagen <url> <count> [opts]` maps to `index`.
    if _needs_index_shim(argv):
        argv = ["index"] + argv

    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    auth = tuple(args.auth.split(":", 1)) if args.auth else None

    try:
        client = SolrClient(args.solr_url, auth=auth)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        version = client.get_version()
    except ConnectionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    except PermissionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print(f"ERROR: Collection '{client.collection}' not found", file=sys.stderr)
        sys.exit(1)

    mode = client.get_mode()
    print(f"Connected to Solr {version} ({mode}) — collection: {client.collection}")

    if args.command == "index":
        _run_index(args, client)
    else:
        _run_reindex(args, client)


# ------------------------------------------------------------------
# Index mode
# ------------------------------------------------------------------

def _run_index(args: argparse.Namespace, client: SolrClient) -> None:
    include_fields = (
        [f.strip() for f in args.include_fields.split(",") if f.strip()]
        if args.include_fields else None
    )

    analyzer = SchemaAnalyzer(client)
    try:
        fields = analyzer.analyze(
            max_fields=args.max_fields,
            fields_per_type=args.fields_per_type,
            include_fields=include_fields,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    if not fields:
        print("ERROR: No indexable fields found (stored=true or docValues=true)", file=sys.stderr)
        sys.exit(1)

    analyzer.print_summary(fields)

    if args.dry_run:
        print("\n--dry-run: stopping before indexing.")
        return

    if args.count <= 0:
        print("Nothing to index (count=0).")
        return

    unique_key_field = next((f.name for f in fields if f.is_unique_key), None)
    if unique_key_field is None:
        print("WARNING: No unique key field found; documents may collide.", file=sys.stderr)
        unique_key_field = ""

    generator = DataGenerator(fields, unique_key_field, seed=args.seed)
    source = GeneratedSource(generator, args.count, args.batch_size)
    progress = ProgressTracker(args.count)
    indexer = DocumentIndexer(
        solr_client=client,
        source=source,
        progress=progress,
        commit_within_ms=args.commit_within,
        max_workers=args.workers,
    )

    def _sigint_handler(sig, frame):
        print("\nInterrupted — stopping…")
        indexer.stop()

    signal.signal(signal.SIGINT, _sigint_handler)

    print(f"\nIndexing {args.count:,} documents (batch={args.batch_size}, workers={args.workers})…\n")
    indexer.run()
    progress.print_summary()

    try:
        count = client.get_doc_count()
        print(f"Collection now contains {count:,} documents.")
    except Exception:
        pass

    if progress.failed_batches > 0:
        sys.exit(2)


# ------------------------------------------------------------------
# Reindex mode
# ------------------------------------------------------------------

def _run_reindex(args: argparse.Namespace, client: SolrClient) -> None:
    try:
        schema = prepare_reindex_schema(client)
    except Exception as exc:
        print(f"ERROR: Schema fetch failed: {exc}", file=sys.stderr)
        sys.exit(1)

    ok, reason = is_sortable_for_cursor(schema.unique_key_field_info, schema.unique_key_field_type)
    if not ok:
        print(f"ERROR: Cannot reindex — {reason}", file=sys.stderr)
        sys.exit(1)

    source = ReindexSource(
        client,
        unique_key=schema.unique_key,
        copy_field_rules=schema.copy_field_rules,
        fq=args.fq,
        read_batch_size=args.read_batch_size,
        post_batch_size=args.batch_size,
    )

    try:
        total = source.total()
    except Exception as exc:
        print(f"ERROR: Could not determine document count: {exc}", file=sys.stderr)
        sys.exit(1)

    if total == 0:
        fq_note = f" matching '{args.fq}'" if args.fq else ""
        print(f"No documents found{fq_note}.")
        return

    fq_note = f" (fq={args.fq!r})" if args.fq else ""
    print(
        f"\nReindexing {total:,} documents{fq_note} "
        f"(read-batch={args.read_batch_size}, post-batch={args.batch_size}, workers={args.workers})…\n"
    )

    progress = ProgressTracker(total)
    indexer = DocumentIndexer(
        solr_client=client,
        source=source,
        progress=progress,
        commit_within_ms=args.commit_within,
        max_workers=args.workers,
    )

    def _sigint_handler(sig, frame):
        print("\nInterrupted — stopping…")
        indexer.stop()

    signal.signal(signal.SIGINT, _sigint_handler)

    try:
        indexer.run()
    except RuntimeError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        progress.print_summary()
        sys.exit(1)

    progress.print_summary()

    try:
        count = client.get_doc_count()
        print(f"Collection now contains {count:,} documents.")
    except Exception:
        pass

    if progress.failed_batches > 0:
        sys.exit(2)
