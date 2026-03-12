# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Single-file Python CLI tool (`rdfpress.py`) that bulk-converts RDF/XML files to queryable JSONL, gzipped JSONL, Parquet, or standards-compliant JSON-LD using rdflib. Uses Click for CLI, Rich for progress/output, and `ProcessPoolExecutor` for parallelism. Supports local and remote inputs via fsspec (FTP, S3, etc.).

## Running

Requires [uv](https://docs.astral.sh/uv/) — no manual dependency installation needed:

```bash
uv run rdfpress.py single input.xml           # single file → simplified JSON
uv run rdfpress.py single input.xml --jsonld   # single file → JSON-LD
uv run rdfpress.py batch input_dir/ -o out_dir/             # directory → gzipped JSONL
uv run rdfpress.py batch zip_folder/ -o out_dir/ -w 8      # folder of zips, 8 workers
uv run rdfpress.py batch ftp://user:pass@host/path/ -o out/ # remote zips via fsspec
```

## Architecture

The entire codebase is a single script with inline PEP 723 script metadata (dependencies declared in the `# /// script` header). It's also installable as a package via `pyproject.toml` (hatchling backend).

**Processing pipeline:** RDF/XML → rdflib Graph → JSON-LD serialization → optional simplification → output

Key functions:
- `parse_rdfxml(source, jsonld)` — core parser, accepts file path or raw bytes
- `rekey_by_type(data)` — restructures `@graph` array into dict keyed by `@type` (simplified mode only)
- `simplify_value(v)` / `simplify_node(node)` — unwrap JSON-LD value containers losslessly
- `_parse_chunk(zip_path, entry_names, jsonld)` — worker function that opens zip independently per process (avoids pickling bytes across process boundaries)
- `_batch_from_zips(...)` — processes zips sequentially through a shared worker pool with adaptive chunking
- `_batch_single(...)` — parallel processing for directory/single-zip inputs
- `_export_formats(...)` — converts JSONL intermediate to requested formats (jsonl, jsonl.gz, parquet)
- `_batch_from_zips_remote(...)` — downloads remote zips via fsspec, delegates to local pipeline

**Output pipeline:** Batch always writes an uncompressed JSONL intermediate first, then exports to each requested format (`--format jsonl,jsonl.gz,parquet`). Output is stored in per-format subdirectories under the output directory. DuckDB is used for JSONL→Parquet conversion.

**Parallelism model:** For zip inputs, adaptive chunking (`_adaptive_chunk_size`) targets `workers*8` tasks per zip. Workers re-open the zip file independently to avoid serializing file contents across processes. Memory is bounded to O(1 zip) regardless of batch size.

## Key conventions

- Python ≥ 3.11 required (uses `str | bytes` union syntax, etc.)
- No test suite currently exists
- `rdflib.term` logger is silenced at ERROR level (noisy but harmless warnings about XSD type conversions)
- Batch output uses atomic write pattern: write to `.tmp`, rename on success
- `--resume` flag enables crash recovery by skipping zips with existing output files
