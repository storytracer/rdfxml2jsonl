# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "rdflib>=7.0",
#     "click>=8.0",
#     "rich>=13.0",
#     "fsspec>=2026.2.0",
#     "orjson>=3.10",
#     "duckdb>=1.0",
# ]
# ///
"""
rdfxml2jsonl — Bulk-convert RDF/XML to queryable JSONL or JSON-LD.

Parses RDF/XML files using rdflib and outputs either simplified JSON
for efficient bulk processing, or standards-compliant JSON-LD suitable
for use in RDF pipelines.

OUTPUT MODES
============

Simplified JSON (default):
    Optimised for analytical querying in tools like DuckDB, jq, Pandas,
    or Polars. The flat @graph array is restructured so that each RDF
    class becomes a top-level key, and JSON-LD wrappers are stripped
    to produce compact, directly queryable output.

    This is a one-way transformation — the result is NOT valid JSON-LD
    and cannot be round-tripped back to RDF without the information
    that was removed. Specifically:

      - @context is dropped (prefixed keys are baked into property names).
      - @type is removed from nodes (already encoded as the grouping key).
      - {"@id": "http://..."} references are unwrapped to plain strings.
      - {"@value": 42, "@type": "xsd:long"} typed literals are unwrapped
        to their native JSON types.
      - Language-tagged strings are preserved as-is because the tag
        carries information that would otherwise be lost:
          {"@value": "example", "@language": "en"} -> (unchanged)

JSON-LD (--jsonld):
    Standards-compliant JSON-LD that can be loaded by any JSON-LD
    processor and round-tripped back to RDF. The @context, @type,
    @id references, and typed value wrappers are all preserved exactly
    as rdflib serialises them. The compact context is derived from each
    file's own xmlns namespace declarations.

Usage:
    uv run rdfxml2jsonl.py single input.xml
    uv run rdfxml2jsonl.py single input.xml --jsonld
    uv run rdfxml2jsonl.py batch  input_dir/ -o output.jsonl.gz
    uv run rdfxml2jsonl.py batch  archive.zip -o output.jsonl.gz --jsonld
    uv run rdfxml2jsonl.py batch  zip_folder/ -o out_dir/ -w 8
"""

import dataclasses
import duckdb
import gzip
import os
import orjson
import logging
import shutil
import sys
import zipfile
from io import BytesIO
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Generator

import fsspec
from fsspec.implementations.local import LocalFileSystem

import click
from rdflib import Graph
from rdflib.plugins.serializers.jsonld import from_rdf
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn
from rich.table import Table

console = Console(stderr=False)
err_console = Console(stderr=True)

_MAX_ERRORS_SHOWN = 3  # max per-file errors shown per zip

# rdflib logs noisy tracebacks for certain literal type conversions that fail
# (e.g. values that don't match their declared XSD datatype). The original
# lexical form is always preserved — no data is lost — so we silence these.
logging.getLogger("rdflib.term").setLevel(logging.ERROR)


# ---------------------------------------------------------------------------
# Namespace / context helpers
# ---------------------------------------------------------------------------

def context_from_graph(g: Graph) -> dict:
    """
    Build a JSON-LD context dict from the namespace bindings that rdflib
    discovered while parsing. This produces compact prefixed property
    names without requiring a hardcoded context.
    """
    return {
        prefix: str(uri)
        for prefix, uri in g.namespace_manager.namespaces()
        if prefix  # skip the default empty prefix
    }


# ---------------------------------------------------------------------------
# Value simplification (used only in simplified JSON mode)
# ---------------------------------------------------------------------------

def simplify_value(v: Any) -> Any:
    """
    Recursively unwrap JSON-LD value containers where the unwrapping is
    lossless.

    Unwrapped (no information lost):
      {"@id": "http://..."}                    ->  "http://..."
      {"@value": 42, "@type": "xsd:long"}      ->  42
      {"@value": "hello"}                       ->  "hello"

    Kept as-is (language tag carries information):
      {"@value": "example", "@language": "en"}  ->  unchanged

    All other dicts and lists are recursed into.
    """
    if isinstance(v, dict):
        keys = set(v.keys())

        # Pure URI reference
        if keys == {"@id"}:
            return v["@id"]

        # Typed or plain literal without a language tag
        if "@value" in keys and "@language" not in keys:
            return v["@value"]

        # Anything else: recurse
        return {k: simplify_value(val) for k, val in v.items()}

    if isinstance(v, list):
        return [simplify_value(item) for item in v]

    return v


def simplify_node(node: dict) -> dict:
    """
    Simplify a single graph node: strip @type (it is redundant because
    it already serves as the grouping key) and unwrap all nested values.
    """
    return {
        k: simplify_value(v)
        for k, v in node.items()
        if k != "@type"
    }


# ---------------------------------------------------------------------------
# Graph restructuring (used only in simplified JSON mode)
# ---------------------------------------------------------------------------

def rekey_by_type(data: dict) -> dict:
    """
    Restructure a JSON-LD document by replacing the flat @graph array
    with a dict keyed by each node's @type.

    - Single-occurrence types  -> stored as an object
    - Multi-occurrence types   -> stored as an array
    - Nodes without @type      -> collected under "_untyped"
    - Nodes with multiple types appear under each type key

    The @context key is dropped from the output since prefixed keys are
    already baked into the property names.
    """
    graph = data.get("@graph", [])
    if not graph:
        return data

    grouped: dict[str, list[dict]] = {}

    for node in graph:
        raw_type = node.get("@type")

        if raw_type is None:
            types = ["_untyped"]
        elif isinstance(raw_type, list):
            types = raw_type
        else:
            types = [raw_type]

        for t in types:
            grouped.setdefault(t, []).append(node)

    result: dict[str, Any] = {}

    for type_key, nodes in grouped.items():
        nodes = [simplify_node(n) for n in nodes]
        result[type_key] = nodes

    return result


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_rdfxml(source: str | bytes, jsonld: bool = False) -> dict:
    """
    Parse an RDF/XML source and return a dict.

    Args:
        source: A filesystem path (str) or raw XML bytes.
        jsonld: If True, return valid JSON-LD with all wrappers intact.
                If False (default), return simplified JSON keyed by @type.
    """
    g = Graph()

    if isinstance(source, bytes):
        g.parse(BytesIO(source), format="xml")
    else:
        g.parse(source, format="xml")

    context = context_from_graph(g)
    data = from_rdf(g, context_data=context)

    if jsonld:
        return data

    return rekey_by_type(data)


# ---------------------------------------------------------------------------
# File iterators (directory and zip)
# ---------------------------------------------------------------------------

def iter_xml_from_dir(
    dir_path: Path, pattern: str
) -> Generator[tuple[str, str | bytes], None, None]:
    """Yield (filename, filepath) for files matching *pattern* in a directory."""
    for f in sorted(dir_path.glob(pattern)):
        yield f.name, str(f)


def iter_xml_from_zip(
    zip_path: Path, pattern: str
) -> Generator[tuple[str, str | bytes], None, None]:
    """Yield (filename, xml_bytes) for matching entries inside a zip archive."""
    suffix = pattern.replace("*", "")  # e.g. "*.xml" -> ".xml"
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = sorted(
            n for n in zf.namelist()
            if n.endswith(suffix) and not n.startswith("__MACOSX")
        )
        for name in members:
            yield Path(name).name, zf.read(name)


# ---------------------------------------------------------------------------
# Parallel processing workers
# ---------------------------------------------------------------------------

_CHUNK_SIZE_MIN = 100    # floor: below this IPC overhead per task dominates
_CHUNK_SIZE_MAX = 10_000 # ceiling: cap per-chunk result size (~50 MB pickled)


def _adaptive_chunk_size(n_entries: int, workers: int) -> int:
    """Choose chunk size targeting ~workers*8 tasks per zip.

    Fewer, larger chunks reduce zip-central-directory re-reads; more, smaller
    chunks keep all workers busy.  Clamped to [_CHUNK_SIZE_MIN, _CHUNK_SIZE_MAX].
    """
    target_chunks = max(1, workers) * 8
    size = n_entries // target_chunks if target_chunks else n_entries
    return max(_CHUNK_SIZE_MIN, min(_CHUNK_SIZE_MAX, size))


def _init_worker():
    """Silence noisy rdflib warnings in worker processes."""
    logging.getLogger("rdflib.term").setLevel(logging.ERROR)


def _parse_one(
    name: str, source: str | bytes, jsonld: bool,
) -> tuple[str, str | None, str | None]:
    """Worker: parse one RDF/XML file, return (name, json_line, error_msg)."""
    try:
        data = parse_rdfxml(source, jsonld=jsonld)
        line = orjson.dumps(data).decode()
        return (name, line, None)
    except Exception as e:
        return (name, None, f"{name} — {e}")


def _parse_chunk(
    zip_path: str, entry_names: list[str], jsonld: bool,
) -> list[tuple[str, str | None, str | None]]:
    """Worker: open a zip, read + parse a chunk of entries.

    Each worker opens the zip independently so no bytes are pickled across
    process boundaries.  Returns [(source_name, json_line | None, error | None), ...].
    """
    results: list[tuple[str, str | None, str | None]] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for entry_name in entry_names:
            source_name = Path(entry_name).name
            try:
                xml_bytes = zf.read(entry_name)
                data = parse_rdfxml(xml_bytes, jsonld=jsonld)
                line = orjson.dumps(data).decode()
                results.append((source_name, line, None))
            except Exception as e:
                results.append((source_name, None, f"{source_name} — {e}"))
    return results


# ---------------------------------------------------------------------------
# Batch stats / reporting
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class ZipRow:
    """Per-zip result used to build the summary table."""
    stem: str
    ok: int = 0
    fail: int = 0
    produced: dict[str, Path] = dataclasses.field(default_factory=dict)
    errors: list[str] = dataclasses.field(default_factory=list)
    status: str = "ok"       # "ok" | "skip" | "error"
    reason: str = ""         # reason string when status != "ok"


@dataclasses.dataclass
class BatchStats:
    """Aggregated batch results returned by _batch_from_zips."""
    skipped_resume: int = 0
    skipped_empty: int = 0
    skipped_bad: int = 0
    rows: list[ZipRow] = dataclasses.field(default_factory=list)

    @property
    def ok(self) -> int:
        return sum(r.ok for r in self.rows)

    @property
    def fail(self) -> int:
        return sum(r.fail for r in self.rows)

    @property
    def n_processed(self) -> int:
        return sum(1 for r in self.rows if r.status == "ok")

    def merge(self, other: "BatchStats") -> None:
        """Merge *other* into this instance (used by remote batching)."""
        self.skipped_resume += other.skipped_resume
        self.skipped_empty += other.skipped_empty
        self.skipped_bad += other.skipped_bad
        self.rows.extend(other.rows)


def _print_batch_report(stats: BatchStats, formats: list[str]) -> None:
    """Print a rich table summarising all processed zips."""
    ok_rows = [r for r in stats.rows if r.status == "ok"]

    if ok_rows:
        table = Table(
            show_header=True, header_style="bold", box=None,
            pad_edge=False, padding=(0, 2),
        )
        table.add_column("Zip")
        table.add_column("Records", justify="right")
        for fmt in formats:
            table.add_column(fmt, justify="right")
        table.add_column("Errors", justify="right")

        for r in ok_rows:
            sizes = []
            for fmt in formats:
                if fmt in r.produced and r.produced[fmt].exists():
                    mb = r.produced[fmt].stat().st_size / (1024 * 1024)
                    sizes.append(f"{mb:.1f} MB")
                else:
                    sizes.append("–")
            err_cell = f"[red]{r.fail:,}[/]" if r.fail else "[dim]–[/]"
            table.add_row(r.stem, f"{r.ok:,}", *sizes, err_cell)

        console.print()
        console.print(table)

    # Errors detail (below table)
    for r in ok_rows:
        if r.errors:
            console.print(f"  [dim]{r.stem}:[/]")
            for e in r.errors[:_MAX_ERRORS_SHOWN]:
                console.print(f"    [red]>[/] {e[:120]}")
            if len(r.errors) > _MAX_ERRORS_SHOWN:
                console.print(
                    f"    [dim]… {len(r.errors) - _MAX_ERRORS_SHOWN} "
                    f"more errors suppressed[/]"
                )

    # Skip rows (if any)
    skip_rows = [r for r in stats.rows if r.status in ("skip", "error")]
    for r in skip_rows:
        console.print(f"  [yellow]SKIP[/] {r.stem}: {r.reason}")

    # Summary line
    console.print()
    parts = []
    if stats.skipped_resume:
        parts.append(f"{stats.skipped_resume} skipped (resume)")
    if stats.skipped_empty:
        parts.append(f"{stats.skipped_empty} skipped (empty)")
    if stats.skipped_bad:
        parts.append(f"{stats.skipped_bad} skipped (bad)")
    summary = (
        f"[bold]Done.[/] {stats.n_processed} zip(s), "
        f"[green]{stats.ok:,}[/] records converted"
    )
    if stats.fail:
        summary += f", [red]{stats.fail:,}[/] failed"
    summary += "."
    if parts:
        summary += f"  [dim]({', '.join(parts)})[/]"
    console.print(summary)


# ---------------------------------------------------------------------------
# Batch helpers
# ---------------------------------------------------------------------------

def _batch_from_zips(
    zip_stems: list[tuple[Path, str]],
    out_dir: Path,
    formats: list[str],
    jsonld: bool, pattern: str, workers: int,
    compresslevel: int = 6, resume: bool = False,
) -> BatchStats:
    """Process zip files sequentially with parallel chunk parsing within each.

    Zips are processed one at a time through a shared worker pool.  This
    bounds memory and open file handles to O(1 zip) regardless of batch
    size, while keeping all CPU cores saturated via adaptive chunking.

    An uncompressed JSONL intermediate is always written first, then
    converted to each requested output format (jsonl, jsonl.gz, parquet).
    Output is stored in per-format subdirectories under *out_dir*.

    Returns a BatchStats with per-zip results; the caller is responsible
    for printing the report via ``_print_batch_report``.
    """
    suffix = pattern.replace("*", "")  # e.g. "*.xml" -> ".xml"
    pool_size = max(1, workers)
    stats = BatchStats()

    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Phase 0: filter (resume) and clean stale intermediates ---
    to_process: list[tuple[Path, str]] = []

    for zp, stem in zip_stems:
        # Clean stale intermediate / tmp files
        for stale_suffix in (".jsonl.tmp", ".jsonl"):
            stale = out_dir / f"{stem}{stale_suffix}"
            if stale.exists():
                stale.unlink()
        if resume and _stem_complete(stem, out_dir, formats):
            stats.skipped_resume += 1
            continue
        to_process.append((zp, stem))

    if not to_process:
        if stats.skipped_resume:
            console.print("[dim]Nothing to process — all outputs already exist.[/]")
        return stats

    # --- Phase 1: process each zip through a shared pool ---
    n_to_do = len(to_process)
    current_tmp: Path | None = None

    pool = (
        ProcessPoolExecutor(max_workers=pool_size, initializer=_init_worker)
        if pool_size > 1
        else None
    )

    try:
        for zip_idx, (zp, stem) in enumerate(to_process, 1):
            # 1a. Read central directory --------------------------------
            try:
                zf_handle = zipfile.ZipFile(zp, "r")
            except (zipfile.BadZipFile, EOFError, OSError) as exc:
                stats.skipped_bad += 1
                stats.rows.append(ZipRow(
                    stem=stem, status="skip", reason=str(exc),
                ))
                continue

            with zf_handle:
                members = sorted(
                    n for n in zf_handle.namelist()
                    if n.endswith(suffix) and not n.startswith("__MACOSX")
                )

            if not members:
                stats.skipped_empty += 1
                stats.rows.append(ZipRow(
                    stem=stem, status="skip",
                    reason=f"no entries matching '{pattern}'",
                ))
                continue

            # 1b. Adaptive chunking -------------------------------------
            chunk_size = _adaptive_chunk_size(len(members), pool_size)
            chunks = [
                members[i : i + chunk_size]
                for i in range(0, len(members), chunk_size)
            ]

            # 1c. Process chunks → write uncompressed JSONL intermediate -
            tmp_path = out_dir / f"{stem}.jsonl.tmp"
            jsonl_path = out_dir / f"{stem}.jsonl"
            current_tmp = tmp_path
            row = ZipRow(stem=stem)
            zp_str = str(zp)

            try:
                with open(tmp_path, "w", encoding="utf-8") as f:
                    with Progress(
                        SpinnerColumn(),
                        TextColumn(
                            f"[dim]\\[{zip_idx}/{n_to_do}][/] {zp.name}"
                        ),
                        BarColumn(bar_width=30),
                        MofNCompleteColumn(),
                        TimeRemainingColumn(),
                        console=console,
                        transient=True,
                    ) as progress:
                        task = progress.add_task("parse", total=len(members))

                        if pool is None:
                            # Sequential (workers=1)
                            for chunk_names in chunks:
                                results = _parse_chunk(zp_str, chunk_names, jsonld)
                                for _n, line, error in results:
                                    if line is not None:
                                        f.write(line)
                                        f.write("\n")
                                        row.ok += 1
                                    else:
                                        row.errors.append(error)
                                        row.fail += 1
                                progress.update(task, advance=len(chunk_names))
                        else:
                            # Parallel
                            futures = {
                                pool.submit(_parse_chunk, zp_str, names, jsonld): names
                                for names in chunks
                            }
                            for fut in as_completed(futures):
                                try:
                                    results = fut.result()
                                except Exception as exc:
                                    failed_names = futures[fut]
                                    row.errors.append(
                                        f"chunk ({len(failed_names)} entries): {exc}"
                                    )
                                    row.fail += len(failed_names)
                                    progress.update(task, advance=len(failed_names))
                                    continue

                                for _n, line, error in results:
                                    if line is not None:
                                        f.write(line)
                                        f.write("\n")
                                        row.ok += 1
                                    else:
                                        row.errors.append(error)
                                        row.fail += 1
                                progress.update(task, advance=len(results))

                # 1d. Atomic rename of intermediate ----------------------
                tmp_path.rename(jsonl_path)
                current_tmp = None

                # 1e. Export to requested formats -------------------------
                row.produced = _export_formats(
                    jsonl_path, stem, out_dir, formats, compresslevel,
                )
                jsonl_path.unlink()

            except Exception as exc:
                if tmp_path.exists():
                    tmp_path.unlink()
                if jsonl_path.exists():
                    jsonl_path.unlink()
                current_tmp = None
                stats.skipped_bad += 1
                row.status = "error"
                row.reason = str(exc)
                stats.rows.append(row)
                continue

            stats.rows.append(row)
            del members, chunks

    except KeyboardInterrupt:
        err_console.print("\n[bold red]Interrupted.[/]")
        if current_tmp is not None and current_tmp.exists():
            current_tmp.unlink()
            err_console.print(f"Cleaned up: {current_tmp.name}")
        err_console.print(
            "[dim]Re-run with --resume to continue where you left off.[/]"
        )
        sys.exit(130)

    finally:
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)

    return stats


def _batch_single(
    entries: list[tuple[str, str | bytes]],
    stem: str, out_dir: Path,
    formats: list[str],
    jsonld: bool, workers: int, compresslevel: int = 6,
):
    """Parse a list of (name, source) entries into output files.

    Writes an uncompressed JSONL intermediate first, then converts to each
    requested format (jsonl, jsonl.gz, parquet) in per-format subdirectories.
    """
    pool_size = min(workers, len(entries))
    success, failed = 0, 0

    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = out_dir / f"{stem}.jsonl.tmp"
    jsonl_path = out_dir / f"{stem}.jsonl"

    errors: list[str] = []

    with open(tmp_path, "w", encoding="utf-8") as f:
        with Progress(
            SpinnerColumn(),
            TextColumn("Converting"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task("convert", total=len(entries))

            if pool_size <= 1:
                for name, source in entries:
                    try:
                        data = parse_rdfxml(source, jsonld=jsonld)
                        f.write(orjson.dumps(data).decode())
                        f.write("\n")
                        success += 1
                    except Exception as e:
                        errors.append(f"{name} — {e}")
                        failed += 1
                    progress.update(task, advance=1)
            else:
                with ProcessPoolExecutor(max_workers=pool_size, initializer=_init_worker) as pool:
                    futures = {
                        pool.submit(_parse_one, name, source, jsonld): name
                        for name, source in entries
                    }
                    for fut in as_completed(futures):
                        name, line, error = fut.result()
                        if line is not None:
                            f.write(line)
                            f.write("\n")
                            success += 1
                        else:
                            errors.append(error)
                            failed += 1
                        progress.update(task, advance=1)

    tmp_path.rename(jsonl_path)

    # Export to requested formats
    produced = _export_formats(jsonl_path, stem, out_dir, formats, compresslevel)
    jsonl_path.unlink()

    console.print()
    console.print(
        f"[bold]Done.[/] [green]{success:,}[/] converted"
        + (f", [red]{failed:,}[/] failed" if failed else "")
        + f" out of {len(entries):,} files."
    )
    if errors:
        for e in errors[:_MAX_ERRORS_SHOWN]:
            console.print(f"  [red]>[/] {e[:120]}")
        if len(errors) > _MAX_ERRORS_SHOWN:
            console.print(
                f"  [dim]… {len(errors) - _MAX_ERRORS_SHOWN} more errors suppressed[/]"
            )
    if produced:
        for fmt, p in produced.items():
            size_mb = p.stat().st_size / (1024 * 1024)
            console.print(f"  {fmt}: {p} ({size_mb:.1f} MB)")


# ---------------------------------------------------------------------------
# Multi-format export helpers
# ---------------------------------------------------------------------------

_FMT_EXT = {"jsonl": ".jsonl", "jsonl.gz": ".jsonl.gz", "parquet": ".parquet"}


def _stem_complete(stem: str, out_dir: Path, formats: list[str]) -> bool:
    """Check whether all requested format outputs exist for a given stem."""
    return all(
        (out_dir / fmt / f"{stem}{_FMT_EXT[fmt]}").exists()
        for fmt in formats
    )


def _to_parquet(jsonl_path: Path, parquet_path: Path) -> None:
    """Convert an uncompressed JSONL file to Parquet via DuckDB."""
    if jsonl_path.stat().st_size == 0:
        return
    tmp = parquet_path.with_suffix(".parquet.tmp")
    try:
        src = str(jsonl_path).replace("'", "''")
        dst = str(tmp).replace("'", "''")
        duckdb.sql(
            f"COPY (SELECT COLUMNS(*)::JSON[] FROM read_json('{src}', "
            f"maximum_depth=1, sample_size=-1, union_by_name=true)) "
            f"TO '{dst}' (FORMAT PARQUET)"
        )
        tmp.rename(parquet_path)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def _compress_gzip(src: Path, dst: Path, compresslevel: int) -> None:
    """Gzip-compress *src* to *dst* with atomic write."""
    tmp = Path(str(dst) + ".tmp")
    try:
        with open(src, "rb") as f_in, gzip.open(tmp, "wb", compresslevel=compresslevel) as f_out:
            shutil.copyfileobj(f_in, f_out)
        tmp.rename(dst)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def _export_formats(
    jsonl_path: Path, stem: str, out_dir: Path,
    formats: list[str], compresslevel: int,
) -> dict[str, Path]:
    """Produce each requested format from the uncompressed JSONL intermediate.

    Returns dict mapping format name to output path.
    """
    produced: dict[str, Path] = {}
    for fmt in formats:
        fmt_dir = out_dir / fmt
        fmt_dir.mkdir(parents=True, exist_ok=True)
        if fmt == "jsonl":
            dest = fmt_dir / f"{stem}.jsonl"
            shutil.copy2(jsonl_path, dest)
            produced[fmt] = dest
        elif fmt == "jsonl.gz":
            dest = fmt_dir / f"{stem}.jsonl.gz"
            _compress_gzip(jsonl_path, dest, compresslevel)
            produced[fmt] = dest
        elif fmt == "parquet":
            dest = fmt_dir / f"{stem}.parquet"
            _to_parquet(jsonl_path, dest)
            produced[fmt] = dest
    return produced


# ---------------------------------------------------------------------------
# Remote storage helpers (fsspec)
# ---------------------------------------------------------------------------

def _batch_from_zips_remote(
    fs: fsspec.AbstractFileSystem,
    zip_paths: list[str],
    out_dir: Path,
    cache_dir: Path,
    formats: list[str],
    jsonld: bool,
    pattern: str,
    workers: int,
    compresslevel: int,
    resume: bool,
):
    """Download remote ZIP files to a local cache, then process them.

    Each ZIP is downloaded via *fs* to *cache_dir*.  Already-cached files
    are reused without re-downloading.  Processing is delegated to the
    existing ``_batch_from_zips`` pipeline which operates on local paths.
    Results are accumulated and printed as a single report at the end.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)

    n = len(zip_paths)
    combined = BatchStats()

    try:
        for idx, remote_zip in enumerate(zip_paths, 1):
            zip_name = remote_zip.rsplit("/", 1)[-1]
            stem = zip_name.rsplit(".", 1)[0]
            cached_zip = cache_dir / zip_name

            # Resume: skip if all format outputs already exist
            if resume and _stem_complete(stem, out_dir, formats):
                combined.skipped_resume += 1
                continue

            # Download only if not already cached
            tag = f"[dim]\\[{idx}/{n}][/] "
            if not cached_zip.exists():
                try:
                    info = fs.info(remote_zip)
                    size_mb = info.get("size", 0) / (1024 * 1024)
                    console.print(
                        f"{tag}Downloading {zip_name} ({size_mb:.0f} MB)…"
                    )
                except Exception:
                    console.print(f"{tag}Downloading {zip_name}…")

                tmp = cached_zip.with_suffix(".zip.tmp")
                try:
                    with fs.open(remote_zip, "rb") as src, open(tmp, "wb") as dst:
                        shutil.copyfileobj(src, dst)
                    tmp.rename(cached_zip)
                except BaseException:
                    tmp.unlink(missing_ok=True)
                    raise
            else:
                console.print(f"{tag}[dim]cached[/] {zip_name}")

            batch_stats = _batch_from_zips(
                [(cached_zip, stem)],
                out_dir, formats,
                jsonld, pattern, workers, compresslevel, resume=False,
            )
            combined.merge(batch_stats)
            cached_zip.unlink(missing_ok=True)

    except KeyboardInterrupt:
        err_console.print("\n[bold red]Interrupted.[/]")
        err_console.print(
            "[dim]Re-run with --resume to continue where you left off.[/]"
        )
        sys.exit(130)

    _print_batch_report(combined, formats)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.group()
def cli():
    """Convert RDF/XML to simplified JSON or standards-compliant JSON-LD."""


@cli.command()
@click.argument("input_file", type=click.Path(exists=True, dir_okay=False))
@click.option("-o", "--output", type=click.Path(), default=None, help="Output path. Default: input with .json/.jsonld extension.")
@click.option("--jsonld", is_flag=True, help="Output valid JSON-LD instead of simplified JSON.")
def single(input_file: str, output: str | None, jsonld: bool):
    """Convert a single RDF/XML file to JSON or JSON-LD."""
    data = parse_rdfxml(input_file, jsonld=jsonld)

    if output is None:
        suffix = ".jsonld" if jsonld else ".json"
        output = str(Path(input_file).with_suffix(suffix))

    Path(output).write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2))
    console.print(f"[green]OK[/] {input_file} → {output}")


@cli.command()
@click.argument("input_path", type=str)
@click.option("-o", "--output", type=click.Path(), default=None,
              help="Output directory. Per-format subdirectories are created inside.")
@click.option("--format", "formats", type=str, default="jsonl.gz",
              help="Comma-separated output formats: jsonl, jsonl.gz, parquet. [default: jsonl.gz]")
@click.option("--jsonld", is_flag=True, help="Output valid JSON-LD instead of simplified JSON.")
@click.option("--glob", "pattern", type=str, default="*.xml", help="File glob pattern. [default: *.xml]")
@click.option("-w", "--workers", type=int, default=None, help="Parallel workers. [default: number of CPUs]")
@click.option("--compresslevel", type=click.IntRange(0, 9), default=6, help="Gzip compression level (0=none, 9=max). [default: 6]")
@click.option("--resume", is_flag=True, help="Skip zips whose output already exists.")
@click.option("--cache-dir", type=click.Path(), default=None,
              help="Cache directory for remote downloads. [default: <output>/.cache]")
def batch(input_path: str, output: str | None, formats: str, jsonld: bool,
          pattern: str, workers: int, compresslevel: int, resume: bool,
          cache_dir: str | None):
    """
    Convert many RDF/XML files to JSONL, gzipped JSONL, and/or Parquet.

    INPUT_PATH can be a local path or any fsspec-compatible URL
    (ftp://user:pass@host/path/, s3://bucket/prefix/, …).

    Accepts a directory of XML files, a .zip archive, or a directory
    of .zip archives.  For a single source every input file becomes
    one JSON line in a single output.  For multiple zips each archive
    produces its own output file.

    Output is stored in per-format subdirectories (e.g. out/jsonl.gz/,
    out/parquet/).  Use --format to select one or more formats.

    Credentials for remote storage are embedded in the URL.

    Use --jsonld for standards-compliant JSON-LD output.
    """
    workers = max(1, workers if workers is not None else os.cpu_count() or 4)

    # --- Parse and validate formats ---
    format_list = [f.strip() for f in formats.split(",")]
    valid_formats = {"jsonl", "jsonl.gz", "parquet"}
    if bad := set(format_list) - valid_formats:
        err_console.print(f"[red]Error:[/] unknown format(s): {', '.join(bad)}")
        err_console.print(f"Valid formats: {', '.join(sorted(valid_formats))}")
        sys.exit(1)

    # --- Resolve input via fsspec (handles local paths and remote URLs) ---
    try:
        fs, root = fsspec.url_to_fs(input_path)
    except Exception as exc:
        err_console.print(f"[red]Error:[/] cannot open {input_path}: {exc}")
        sys.exit(1)

    is_local = isinstance(fs, LocalFileSystem)

    if not fs.exists(root):
        err_console.print(f"[red]Error:[/] {input_path} does not exist.")
        sys.exit(1)

    # --- Directory input ---
    if fs.isdir(root):
        raw_ls = fs.ls(root, detail=False)
        root_prefix = root.rstrip("/") + "/"
        zip_files = sorted(
            p if p.startswith(root_prefix)
            else root_prefix + p.rsplit("/", 1)[-1]
            for p in raw_ls if p.endswith(".zip")
        )

        if is_local:
            entries = list(iter_xml_from_dir(Path(root), pattern))

            if not entries and zip_files:
                out_dir = Path(output) if output else Path(root)
                zip_stems = [
                    (Path(zf), Path(zf).stem)
                    for zf in zip_files
                ]
                stats = _batch_from_zips(
                    zip_stems, out_dir, format_list,
                    jsonld, pattern, workers, compresslevel, resume=resume,
                )
                _print_batch_report(stats, format_list)
                return

            if not entries:
                err_console.print(
                    f"[red]Error:[/] no files matching '{pattern}' and no "
                    f".zip files in {input_path}"
                )
                sys.exit(1)

            out_dir = Path(output) if output else Path(root).parent
            stem = Path(root).name
            _batch_single(
                entries, stem, out_dir, format_list,
                jsonld, workers, compresslevel,
            )
            return

        else:
            # Remote directory — must contain zips
            if not zip_files:
                err_console.print(
                    f"[red]Error:[/] no .zip files found at {input_path}"
                )
                sys.exit(1)

            console.print(f"Found {len(zip_files)} zip(s) at {input_path}")
            out_dir = Path(output) if output else Path(".")
            out_dir.mkdir(parents=True, exist_ok=True)
            c_dir = Path(cache_dir) if cache_dir else out_dir / ".cache"
            _batch_from_zips_remote(
                fs, zip_files, out_dir, c_dir,
                format_list, jsonld, pattern, workers, compresslevel, resume,
            )
            return

    # --- Single zip input ---
    if root.endswith(".zip") and fs.isfile(root):
        if is_local:
            out_dir = Path(output) if output else Path(root).parent
            stem = Path(root).stem
            stats = _batch_from_zips(
                [(Path(root), stem)],
                out_dir, format_list,
                jsonld, pattern, workers, compresslevel, resume=resume,
            )
            _print_batch_report(stats, format_list)
        else:
            out_dir = Path(output) if output else Path(".")
            out_dir.mkdir(parents=True, exist_ok=True)
            c_dir = Path(cache_dir) if cache_dir else out_dir / ".cache"
            _batch_from_zips_remote(
                fs, [root], out_dir, c_dir,
                format_list, jsonld, pattern, workers, compresslevel, resume,
            )
        return

    err_console.print(f"[red]Error:[/] {input_path} is not a directory or zip file.")
    sys.exit(1)


if __name__ == "__main__":
    cli()