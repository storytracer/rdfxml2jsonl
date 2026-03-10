# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "rdflib>=7.0",
#     "click>=8.0",
#     "tqdm>=4.0",
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
"""

import gzip
import json
import logging
import sys
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any, Generator

import click
from rdflib import Graph
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
    jsonld_str = g.serialize(format="json-ld", indent=None, context=context)
    data = json.loads(jsonld_str)

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
# CLI
# ---------------------------------------------------------------------------

@click.group()
def cli():
    """Convert RDF/XML to simplified JSON or standards-compliant JSON-LD."""


@cli.command()
@click.argument("input_file", type=click.Path(exists=True, dir_okay=False))
@click.option("-o", "--output", type=click.Path(), default=None, help="Output path. Default: input with .json/.jsonld extension.")
@click.option("--indent", type=int, default=2, help="JSON indent level.")
@click.option("--jsonld", is_flag=True, help="Output valid JSON-LD instead of simplified JSON.")
def single(input_file: str, output: str | None, indent: int, jsonld: bool):
    """Convert a single RDF/XML file to JSON or JSON-LD."""
    data = parse_rdfxml(input_file, jsonld=jsonld)

    if output is None:
        suffix = ".jsonld" if jsonld else ".json"
        output = str(Path(input_file).with_suffix(suffix))

    Path(output).write_text(json.dumps(data, indent=indent, ensure_ascii=False), encoding="utf-8")
    click.echo(f"Converted: {input_file} -> {output}")


@cli.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.option("-o", "--output", type=click.Path(), default=None, help="Output file. Default: <input>.jsonl.gz")
@click.option("--jsonld", is_flag=True, help="Output valid JSON-LD instead of simplified JSON.")
@click.option("--glob", "pattern", type=str, default="*.xml", help="File glob pattern. [default: *.xml]")
def batch(input_path: str, output: str | None, jsonld: bool, pattern: str):
    """
    Convert many RDF/XML files to a single gzipped JSONL file.

    INPUT_PATH can be a directory or a .zip archive. Each input file
    becomes one JSON line in the output. A "_source_file" field is
    added to every record for traceability.

    By default, output is simplified JSON for efficient bulk querying.
    Use --jsonld for standards-compliant JSON-LD output.
    """
    src = Path(input_path)

    if src.is_file() and zipfile.is_zipfile(src):
        entries = list(iter_xml_from_zip(src, pattern))
        source_label = f"zip:{src.name}"
    elif src.is_dir():
        entries = list(iter_xml_from_dir(src, pattern))
        source_label = str(src)
    else:
        click.echo(f"Error: {input_path} is not a directory or zip file.", err=True)
        sys.exit(1)

    if not entries:
        click.echo(f"No files matching '{pattern}' in {source_label}", err=True)
        sys.exit(1)

    if output is None:
        output = str(src.with_suffix(".jsonl.gz"))

    success, failed = 0, 0

    with gzip.open(output, "wt", encoding="utf-8", compresslevel=6) as gz:
        for name, source in tqdm(entries, desc="Converting", unit="file"):
            try:
                data = parse_rdfxml(source, jsonld=jsonld)
                data["_source_file"] = name

                gz.write(json.dumps(data, ensure_ascii=False, separators=(",", ":")))
                gz.write("\n")
                success += 1
            except Exception as e:
                tqdm.write(f"FAILED: {name} — {e}")
                failed += 1

    click.echo(f"\nWrote {output}")
    click.echo(f"  {success} converted, {failed} failed out of {len(entries)} files.")

    if success > 0:
        size_mb = Path(output).stat().st_size / (1024 * 1024)
        click.echo(f"  File size: {size_mb:.1f} MB")


if __name__ == "__main__":
    cli()