"""Command-line interface — parse arguments, orchestrate the pipeline."""

from __future__ import annotations

import argparse
import logging
import signal
import sys

from solr_datagen.config import BATCH_SIZE, COMMIT_WITHIN_MS
from solr_datagen.data_generator import DataGenerator
from solr_datagen.indexer import DocumentIndexer
from solr_datagen.progress import ProgressTracker
from solr_datagen.schema_analyzer import SchemaAnalyzer
from solr_datagen.solr_client import SolrClient


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="solr-datagen",
        description="Generate and index realistic documents into Apache Solr.",
    )
    p.add_argument("solr_url", help="Solr collection URL, e.g. http://localhost:8983/solr/my_core")
    p.add_argument("count", type=int, help="Number of documents to generate")
    p.add_argument("-b", "--batch-size", type=int, default=BATCH_SIZE, help="Docs per HTTP request (default: %(default)s)")
    p.add_argument("-c", "--commit-within", type=int, default=COMMIT_WITHIN_MS, help="commitWithin in ms (default: %(default)s)")
    p.add_argument("-f", "--max-fields", type=int, default=20, help="Max fields to select from schema (default: %(default)s)")
    p.add_argument("--fields-per-type", type=int, default=3, help="Max fields per type category (default: %(default)s)")
    p.add_argument("-w", "--workers", type=int, default=4, help="Parallel submission threads (default: %(default)s)")
    p.add_argument("-a", "--auth", default=None, help="Basic auth as user:password")
    p.add_argument("-s", "--seed", type=int, default=None, help="Random seed for reproducibility")
    p.add_argument("--dry-run", action="store_true", help="Analyse schema only, don't index")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    auth = tuple(args.auth.split(":", 1)) if args.auth else None

    # ---- Connect to Solr ----
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

    # ---- Analyse schema ----
    analyzer = SchemaAnalyzer(client)
    fields = analyzer.analyze(max_fields=args.max_fields, fields_per_type=args.fields_per_type)
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

    # ---- Identify unique key ----
    unique_key_field = next((f.name for f in fields if f.is_unique_key), None)
    if unique_key_field is None:
        print("WARNING: No unique key field found; documents may collide.", file=sys.stderr)
        unique_key_field = ""

    # ---- Generate & index ----
    generator = DataGenerator(fields, unique_key_field, seed=args.seed)
    progress = ProgressTracker(args.count)
    indexer = DocumentIndexer(
        solr_client=client,
        generator=generator,
        progress=progress,
        batch_size=args.batch_size,
        commit_within_ms=args.commit_within,
        max_workers=args.workers,
    )

    # Ctrl+C handling: print partial stats, commit, exit cleanly.
    def _sigint_handler(sig, frame):
        print("\nInterrupted — stopping…")
        indexer.stop()

    signal.signal(signal.SIGINT, _sigint_handler)

    print(f"\nIndexing {args.count:,} documents (batch={args.batch_size}, workers={args.workers})…\n")
    indexer.run(args.count)

    progress.print_summary()

    # Report final doc count.
    try:
        count = client.get_doc_count()
        print(f"Collection now contains {count:,} documents.")
    except Exception:
        pass
