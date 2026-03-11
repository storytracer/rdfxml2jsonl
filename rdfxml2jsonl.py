# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "rdflib>=7.0",
#     "click>=8.0",
#     "tqdm>=4.0",
#     "fsspec>=2026.2.0",
#     "orjson>=3.10",
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

import gzip
import os
import orjson
import logging
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
from tqdm import tqdm

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
        result[type_key] = nodes[0] if len(nodes) == 1 else nodes

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
        data["_source_file"] = name
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
                data["_source_file"] = source_name
                line = orjson.dumps(data).decode()
                results.append((source_name, line, None))
            except Exception as e:
                results.append((source_name, None, f"{source_name} — {e}"))
    return results


# ---------------------------------------------------------------------------
# Batch helpers
# ---------------------------------------------------------------------------

def _batch_from_zips(
    zip_outputs: list[tuple[Path, Path]],
    jsonld: bool, pattern: str, workers: int,
    compresslevel: int = 1, resume: bool = False,
):
    """Process zip files sequentially with parallel chunk parsing within each.

    Zips are processed one at a time through a shared worker pool.  This
    bounds memory and open file handles to O(1 zip) regardless of batch
    size, while keeping all CPU cores saturated via adaptive chunking.

    Args:
        zip_outputs: (zip_path, output_path) pairs — one .jsonl.gz per zip.
        jsonld:      Output valid JSON-LD instead of simplified JSON.
        pattern:     Glob pattern for matching entries (e.g. ``"*.xml"``).
        workers:     Number of parallel workers.
        resume:      Skip zips whose output already exists.
    """
    suffix = pattern.replace("*", "")  # e.g. "*.xml" -> ".xml"
    pool_size = max(1, workers)
    n_total = len(zip_outputs)

    # --- Phase 0: filter (resume) and clean stale .tmp files ---
    to_process: list[tuple[Path, Path]] = []
    skipped_resume = 0

    for zp, out in zip_outputs:
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(out) + ".tmp")
        if tmp.exists():
            tmp.unlink()
        if resume and out.exists():
            skipped_resume += 1
            continue
        to_process.append((zp, out))

    to_process.sort(key=lambda pair: pair[0].stat().st_size, reverse=True)

    if skipped_resume:
        click.echo(f"Resuming: skipped {skipped_resume} already-completed zip(s).")
    if not to_process:
        click.echo("Nothing to process — all outputs already exist.")
        return

    # --- Phase 1: process each zip through a shared pool ---
    grand_ok, grand_fail = 0, 0
    skipped_bad, skipped_empty = 0, 0
    n_to_do = len(to_process)
    current_tmp: Path | None = None

    pool = (
        ProcessPoolExecutor(max_workers=pool_size, initializer=_init_worker)
        if pool_size > 1
        else None
    )

    try:
        for zip_idx, (zp, out) in enumerate(to_process, 1):
            # 1a. Read central directory --------------------------------
            try:
                zf_handle = zipfile.ZipFile(zp, "r")
            except (zipfile.BadZipFile, EOFError, OSError) as exc:
                tqdm.write(f"[{zip_idx}/{n_to_do}] SKIP {zp.name}: {exc}")
                skipped_bad += 1
                continue

            with zf_handle:
                members = sorted(
                    n for n in zf_handle.namelist()
                    if n.endswith(suffix) and not n.startswith("__MACOSX")
                )

            if not members:
                tqdm.write(
                    f"[{zip_idx}/{n_to_do}] SKIP {zp.name}: "
                    f"no entries matching '{pattern}'"
                )
                skipped_empty += 1
                continue

            # 1b. Adaptive chunking -------------------------------------
            chunk_size = _adaptive_chunk_size(len(members), pool_size)
            chunks = [
                members[i : i + chunk_size]
                for i in range(0, len(members), chunk_size)
            ]

            # 1c. Process chunks → write to .tmp ------------------------
            tmp_path = Path(str(out) + ".tmp")
            current_tmp = tmp_path
            ok, fail = 0, 0
            zp_str = str(zp)
            desc = (
                f"[{zip_idx}/{n_to_do}] {zp.name}"
            )

            try:
                with gzip.open(
                    str(tmp_path), "wt", encoding="utf-8", compresslevel=compresslevel,
                ) as gz:
                    if pool is None:
                        # Sequential (workers=1)
                        for chunk_names in tqdm(
                            chunks, desc=desc, unit="chunk", leave=False,
                        ):
                            results = _parse_chunk(zp_str, chunk_names, jsonld)
                            for _n, line, error in results:
                                if line is not None:
                                    gz.write(line)
                                    gz.write("\n")
                                    ok += 1
                                else:
                                    tqdm.write(f"  FAILED: {error}")
                                    fail += 1
                    else:
                        # Parallel
                        futures = {
                            pool.submit(_parse_chunk, zp_str, names, jsonld): names
                            for names in chunks
                        }
                        with tqdm(
                            total=len(members), desc=desc,
                            unit="file", leave=False,
                        ) as pbar:
                            for fut in as_completed(futures):
                                try:
                                    results = fut.result()
                                except Exception as exc:
                                    failed_names = futures[fut]
                                    tqdm.write(
                                        f"  CHUNK ERROR ({len(failed_names)} "
                                        f"entries): {exc}"
                                    )
                                    fail += len(failed_names)
                                    pbar.update(len(failed_names))
                                    continue

                                for _n, line, error in results:
                                    if line is not None:
                                        gz.write(line)
                                        gz.write("\n")
                                        ok += 1
                                    else:
                                        tqdm.write(f"  FAILED: {error}")
                                        fail += 1
                                pbar.update(len(results))

                # 1d. Atomic rename on success --------------------------
                tmp_path.rename(out)
                current_tmp = None

            except Exception as exc:
                tqdm.write(f"  ZIP ERROR {zp.name}: {exc}")
                if tmp_path.exists():
                    tmp_path.unlink()
                current_tmp = None
                skipped_bad += 1
                continue

            # 1e. Per-zip summary ---------------------------------------
            size_mb = out.stat().st_size / (1024 * 1024)
            tqdm.write(
                f"  => {out.name}: {ok:,} ok, {fail:,} failed "
                f"({size_mb:.1f} MB)"
            )
            grand_ok += ok
            grand_fail += fail
            del members, chunks

    except KeyboardInterrupt:
        click.echo("\n\nInterrupted.", err=True)
        if current_tmp is not None and current_tmp.exists():
            current_tmp.unlink()
            click.echo(f"Cleaned up: {current_tmp.name}", err=True)
        click.echo(
            "Re-run with --resume to continue where you left off.", err=True,
        )
        sys.exit(130)

    finally:
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)

    # --- Phase 2: final report ---
    click.echo(f"\nDone. Processed {n_to_do} zip(s) of {n_total} total.")
    if skipped_resume:
        click.echo(f"  Skipped (resume):  {skipped_resume}")
    if skipped_empty:
        click.echo(f"  Skipped (empty):   {skipped_empty}")
    if skipped_bad:
        click.echo(f"  Skipped (bad zip): {skipped_bad}")
    click.echo(f"  Records: {grand_ok:,} converted, {grand_fail:,} failed.")


def _batch_single(
    entries: list[tuple[str, str | bytes]], output: str,
    jsonld: bool, workers: int, compresslevel: int = 1,
):
    """Parse a list of (name, source) entries into a single .jsonl.gz."""
    pool_size = min(workers, len(entries))
    success, failed = 0, 0

    with gzip.open(output, "wt", encoding="utf-8", compresslevel=compresslevel) as gz:
        if pool_size <= 1:
            for name, source in tqdm(entries, desc="Converting", unit="file"):
                try:
                    data = parse_rdfxml(source, jsonld=jsonld)
                    data["_source_file"] = name
                    gz.write(orjson.dumps(data).decode())
                    gz.write("\n")
                    success += 1
                except Exception as e:
                    tqdm.write(f"FAILED: {name} — {e}")
                    failed += 1
        else:
            with ProcessPoolExecutor(max_workers=pool_size, initializer=_init_worker) as pool:
                futures = {
                    pool.submit(_parse_one, name, source, jsonld): name
                    for name, source in entries
                }
                with tqdm(total=len(futures), desc="Converting", unit="file") as pbar:
                    for fut in as_completed(futures):
                        name, line, error = fut.result()
                        if line is not None:
                            gz.write(line)
                            gz.write("\n")
                            success += 1
                        else:
                            tqdm.write(f"FAILED: {error}")
                            failed += 1
                        pbar.update(1)

    click.echo(f"\nWrote {output}")
    click.echo(f"  {success} converted, {failed} failed out of {len(entries)} files.")

    if success > 0:
        size_mb = Path(output).stat().st_size / (1024 * 1024)
        click.echo(f"  File size: {size_mb:.1f} MB")


# ---------------------------------------------------------------------------
# Remote storage helpers (fsspec)
# ---------------------------------------------------------------------------

def _batch_from_zips_remote(
    fs: fsspec.AbstractFileSystem,
    zip_paths: list[str],
    out_dir: Path,
    cache_dir: Path,
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
    """
    cache_dir.mkdir(parents=True, exist_ok=True)

    n = len(zip_paths)
    skipped_resume = 0

    try:
        for idx, remote_zip in enumerate(zip_paths, 1):
            zip_name = remote_zip.rsplit("/", 1)[-1]
            stem = zip_name.rsplit(".", 1)[0]
            local_output = out_dir / f"{stem}.jsonl.gz"
            cached_zip = cache_dir / zip_name

            # Resume: skip if output already exists
            if resume and local_output.exists():
                skipped_resume += 1
                continue

            # Download only if not already cached
            if not cached_zip.exists():
                try:
                    info = fs.info(remote_zip)
                    size_mb = info.get("size", 0) / (1024 * 1024)
                    click.echo(
                        f"[{idx}/{n}] Downloading {zip_name} "
                        f"({size_mb:.0f} MB)..."
                    )
                except Exception:
                    click.echo(f"[{idx}/{n}] Downloading {zip_name}...")

                fs.get(remote_zip, str(cached_zip))
            else:
                click.echo(f"[{idx}/{n}] Using cached {zip_name}")

            _batch_from_zips(
                [(cached_zip, local_output)],
                jsonld, pattern, workers, compresslevel, resume=False,
            )

    except KeyboardInterrupt:
        click.echo("\n\nInterrupted.", err=True)
        click.echo(
            "Re-run with --resume to continue where you left off.", err=True,
        )
        sys.exit(130)

    if skipped_resume:
        click.echo(f"Resuming: skipped {skipped_resume} already-completed zip(s).")


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
    click.echo(f"Converted: {input_file} -> {output}")


@cli.command()
@click.argument("input_path", type=str)
@click.option("-o", "--output", type=click.Path(), default=None,
              help="Output file (single source) or directory (folder of zips).")
@click.option("--jsonld", is_flag=True, help="Output valid JSON-LD instead of simplified JSON.")
@click.option("--glob", "pattern", type=str, default="*.xml", help="File glob pattern. [default: *.xml]")
@click.option("-w", "--workers", type=int, default=None, help="Parallel workers. [default: number of CPUs]")
@click.option("--compresslevel", type=click.IntRange(0, 9), default=6, help="Gzip compression level (0=none, 9=max). [default: 1]")
@click.option("--resume", is_flag=True, help="Skip zips whose output already exists.")
@click.option("--cache-dir", type=click.Path(), default=None,
              help="Cache directory for remote downloads. [default: <output>/.cache]")
def batch(input_path: str, output: str | None, jsonld: bool, pattern: str,
          workers: int, compresslevel: int, resume: bool, cache_dir: str | None):
    """
    Convert many RDF/XML files to gzipped JSONL.

    INPUT_PATH can be a local path or any fsspec-compatible URL
    (ftp://user:pass@host/path/, s3://bucket/prefix/, …).

    Accepts a directory of XML files, a .zip archive, or a directory
    of .zip archives.  For a single source every input file becomes
    one JSON line in a single output.  For multiple zips each archive
    produces its own .jsonl.gz file.

    Credentials for remote storage are embedded in the URL.

    A "_source_file" field is added to every record for traceability.
    Use --jsonld for standards-compliant JSON-LD output.
    """
    workers = max(1, workers if workers is not None else os.cpu_count() or 4)

    # --- Resolve input via fsspec (handles local paths and remote URLs) ---
    try:
        fs, root = fsspec.url_to_fs(input_path)
    except Exception as exc:
        click.echo(f"Error: cannot open {input_path}: {exc}", err=True)
        sys.exit(1)

    is_local = isinstance(fs, LocalFileSystem)

    if not fs.exists(root):
        click.echo(f"Error: {input_path} does not exist.", err=True)
        sys.exit(1)

    # --- Directory input ---
    if fs.isdir(root):
        zip_files = sorted(
            p for p in fs.ls(root, detail=False) if p.endswith(".zip")
        )

        if is_local:
            entries = list(iter_xml_from_dir(Path(root), pattern))

            if not entries and zip_files:
                out_dir = Path(output) if output else Path(root)
                zip_outputs = [
                    (Path(zf), out_dir / (Path(zf).stem + ".jsonl.gz"))
                    for zf in zip_files
                ]
                _batch_from_zips(zip_outputs, jsonld, pattern, workers, compresslevel, resume=resume)
                return

            if not entries:
                click.echo(
                    f"Error: no files matching '{pattern}' and no .zip files "
                    f"in {input_path}",
                    err=True,
                )
                sys.exit(1)

            if output is None:
                output = str(Path(root).with_suffix(".jsonl.gz"))
            _batch_single(entries, output, jsonld, workers, compresslevel)
            return

        else:
            # Remote directory — must contain zips
            if not zip_files:
                click.echo(
                    f"Error: no .zip files found at {input_path}", err=True,
                )
                sys.exit(1)

            click.echo(f"Found {len(zip_files)} zip(s) at {input_path}")
            out_dir = Path(output) if output else Path(".")
            out_dir.mkdir(parents=True, exist_ok=True)
            c_dir = Path(cache_dir) if cache_dir else out_dir / ".cache"
            _batch_from_zips_remote(
                fs, zip_files, out_dir, c_dir,
                jsonld, pattern, workers, compresslevel, resume,
            )
            return

    # --- Single zip input ---
    if root.endswith(".zip") and fs.isfile(root):
        if is_local:
            if output is None:
                output = str(Path(root).with_suffix(".jsonl.gz"))
            _batch_from_zips(
                [(Path(root), Path(output))],
                jsonld, pattern, workers, compresslevel, resume=resume,
            )
        else:
            out_dir = Path(output) if output else Path(".")
            out_dir.mkdir(parents=True, exist_ok=True)
            c_dir = Path(cache_dir) if cache_dir else out_dir / ".cache"
            _batch_from_zips_remote(
                fs, [root], out_dir, c_dir,
                jsonld, pattern, workers, compresslevel, resume,
            )
        return

    click.echo(f"Error: {input_path} is not a directory or zip file.", err=True)
    sys.exit(1)


if __name__ == "__main__":
    cli()