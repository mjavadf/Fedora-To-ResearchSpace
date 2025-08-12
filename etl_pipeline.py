#!/usr/bin/env python3
"""ETL pipeline – Fedora → ResearchSpace
=======================================
CLI tool that traverses a Fedora 4 repository, harvests its RDF, converts the
triples to CIDOC CRM / CRMdig using a configuration‑driven rule catalogue
(*Javad Transformation Object* – JTO), and writes **three artefacts per chunk**
into `--out-dir`:

* `source‑NNN.ttl`   — verbatim Fedora Turtle snapshots for audit/provenance
* `dataset‑NNN.trig` — CRM‑aligned triples generated from the rules
* `insert‑NNN.rq`    — self‑contained `INSERT DATA` commands ready for the
  ResearchSpace SPARQL console (no HTTP POSTs are issued by the script)

Features
--------
* **Binary‑aware fetcher** – falls back to `/<uri>/fcr:metadata` when the primary
  resource is a NonRDFSource, ensuring parseable RDF.
* **Streaming & chunking** – memory‑bounded buffer flushed every
  `--chunk-size` resources (default 10 000).
* **Developer switches** – `--max-resources` for quick prototypes, `-v` for
  verbose logging.
* **Basic‑auth** support and broad RDF content negotiation (`Accept` header)
  while still preferring Turtle.

Example 1: basic usage
-------
```bash
python etl_pipeline.py \
  --fedora-base https://datavault.ficlit.unibo.it/repo/rest \
  --root-path   UBOBU/MICROFILM \
  --rules-file  rules.yaml \
  --out-dir     sparql_out \
  --username    [Auth_user] \
  --password    [Auth_pass] \
  --chunk-size  5000 \
  --max-resources 100 -v
```

```powershell
python etl_pipeline.py `
  --fedora-base https://datavault.ficlit.unibo.it/repo/rest `
  --root-path   UBOBU/MICROFILM `
  --rules-file  rules.yaml `
  --out-dir     sparql_out `
  --username    [Auth_user] `
  --password    [Auth_pass] ` 
  --chunk-size  5000 `
  --max-resources 100 -v
```

Example 2: single resource
-------
```powershell
python etl_pipeline.py -v `
  --fedora-base https://datavault.ficlit.unibo.it/repo/rest `
  --root-path UBOBU/MICROFILM/UBO8306198/402163/UBOBU_UBO8306198_402163_0015/ `
  --rules-file rules.yaml `
  --out-dir sparql_out `
  --username [Auth_user] `
  --password [Auth_pass] `
  --max-resources 1
```

The resulting `.rq` files can be copied directly into the ResearchSpace SPARQL
console for ingestion.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import textwrap
import time
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import rdflib
import requests
import yaml

logger = logging.getLogger("etl")
TTL_MIME = "text/turtle"
CONTAINS_PRED = rdflib.URIRef("http://www.w3.org/ns/ldp#contains")

# ---------------------------------------------------------------------------
# Session and networking helpers
# ---------------------------------------------------------------------------

def build_session(creds: Optional[Tuple[str, str]]) -> requests.Session:
    """Return a persistent requests.Session (with optional basic‑auth)."""
    s = requests.Session()
    if creds:
        s.auth = creds
        logger.debug("HTTP basic auth enabled for user %s", creds[0])
    # Prefer Turtle but accept other RDF media types so Fedora can negotiate
    s.headers["Accept"] = (
        "text/turtle, application/ld+json;q=0.9, application/rdf+xml;q=0.8, "
        "text/n3;q=0.8, */*;q=0.1"
    )
    return s


class NotRDF(Exception):
    """Raised when the server responds with a non‑RDF payload."""


def _download(uri: str, session: requests.Session) -> str:
    """Return body text if it *looks* like RDF, else raise NotRDF."""
    resp = session.get(uri, timeout=60)
    resp.raise_for_status()
    ctype = resp.headers.get("Content-Type", "")
    if "text/turtle" not in ctype and "rdf" not in ctype and "ld+json" not in ctype:
        raise NotRDF(f"Content-Type {ctype!r} is not RDF")
    return resp.text


def fetch_rdf(uri: str, session: requests.Session) -> rdflib.Graph:
    """Fetch RDF, retrying the `/fcr:metadata` endpoint for binaries."""
    logger.debug("Fetching %s", uri)
    try:
        data = _download(uri, session)
    except (NotRDF, requests.HTTPError):
        if uri.rstrip("/").endswith("fcr:metadata"):
            raise  # already at metadata endpoint – give up
        alt_uri = uri.rstrip("/") + "/fcr:metadata"
        logger.debug("  ⇢ retrying metadata endpoint %s", alt_uri)
        data = _download(alt_uri, session)
        uri = alt_uri

    g = rdflib.Graph()
    g.parse(data=data, format="turtle", publicID=uri)
    logger.debug("Parsed %d triples from %s", len(g), uri)
    return g

# ---------------------------------------------------------------------------
# Rule handling
# ---------------------------------------------------------------------------

def load_rules(path: Path) -> Dict[str, Dict[str, str]]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    rules = {
        r["source_predicate"]: {"id": r["id"], "template": r["target_pattern"]}
        for r in data["rules"]
    }
    logger.info("Loaded %d mapping rules from %s", len(rules), path)
    return rules


def apply_rules(src: rdflib.Graph, rules) -> List[str]:
    out: List[str] = []
    for s, p, o in src:
        rule = rules.get(str(p))
        if not rule:
            continue
        out.append(rule["template"].replace("?s", s.n3()).replace("?o", o.n3()).rstrip())
    logger.debug("Generated %d target triples", len(out))
    return out

# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------
PREFIX_BLOCK = textwrap.dedent(
    """
    PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    PREFIX dig: <http://www.cidoc-crm.org/cidoc-crm-dig/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    """
)


def flush_chunk(src_blocks: List[str], tgt_triples: List[str], out_dir: Path, idx: int) -> None:
    if not tgt_triples:
        return

    src_path = out_dir / f"source-{idx:03d}.ttl"
    src_path.write_text("\n\n".join(src_blocks), encoding="utf-8")

    trig_path = out_dir / f"dataset-{idx:03d}.trig"
    trig_content = "\n".join(tgt_triples)
    trig_path.write_text(trig_content, encoding="utf-8")

    rq_path = out_dir / f"insert-{idx:03d}.rq"
    sparql = textwrap.dedent(
        f"""{PREFIX_BLOCK}
        INSERT DATA {{
            GRAPH <http://example.org/datavault> {{
        {textwrap.indent(trig_content, ' ' * 12)}
            }}
        }};"""
    )
    rq_path.write_text(sparql, encoding="utf-8")

    logger.info(
        "Chunk %03d – wrote %s (src %d B, tgt %d triples)",
        idx,
        rq_path.name,
        src_path.stat().st_size,
        len(tgt_triples),
    )

# ---------------------------------------------------------------------------
# Crawl / transform loop
# ---------------------------------------------------------------------------

def crawl(base: str, root: str, rules, out: Path, chunk: int, session: requests.Session, max_res: int):
    queue = deque([f"{base.rstrip('/')}/{root.lstrip('/')}"])
    processed = 0
    chunk_idx = 1
    tgt_buf: List[str] = []
    src_buf: List[str] = []
    out.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()

    while queue and (max_res == 0 or processed < max_res):
        uri = queue.popleft()
        try:
            g_src = fetch_rdf(uri, session)
        except Exception as exc:
            logger.error("Failed to fetch %s – %s", uri, exc)
            continue

        for _s, _p, child in g_src.triples((None, CONTAINS_PRED, None)):
            queue.append(str(child))
            logger.debug("  enqueue %s", child)

        src_buf.append(f"# Source 👉 {uri} ({len(g_src)} triples)")
        src_buf.append(g_src.serialize(format="turtle"))

        tgt_buf.extend(apply_rules(g_src, rules))
        processed += 1

        if processed % chunk == 0:
            flush_chunk(src_buf, tgt_buf, out, chunk_idx)
            src_buf.clear()
            tgt_buf.clear()
            chunk_idx += 1

    flush_chunk(src_buf, tgt_buf, out, chunk_idx)

    logger.info(
        "Finished: %d resources → %d chunks in %.1f s (limit=%s)",
        processed,
        chunk_idx,
        time.perf_counter() - t0,
        "∞" if max_res == 0 else max_res,
    )

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_cli(argv=None):
    ap = argparse.ArgumentParser(description="Fedora → ResearchSpace ETL (dev edition)")
    ap.add_argument("--fedora-base", required=True)
    ap.add_argument("--root-path", required=True)
    ap.add_argument("--rules-file", default="rules.yaml")
    ap.add_argument("--out-dir", default="sparql_out", type=Path)
    ap.add_argument("--chunk-size", default=10000, type=int)
    ap.add_argument("--username")
    ap.add_argument("--password")
    ap.add_argument("--max-resources", default=0, type=int, help="Stop after N resources (0 = unlimited)")
    ap.add_argument("-v", "--verbose", action="store_true")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_cli(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

    creds = None
    if args.username:
        pwd = args.password or os.getenv("FEDORA_PASSWORD") or input("Fedora password: ")
        creds = (args.username, pwd)

    session = build_session(creds)
    rules = load_rules(Path(args.rules_file))

    try:
        crawl(
            args.fedora_base,
            args.root_path,
            rules,
            args.out_dir,
            args.chunk_size,
            session,
            args.max_resources,
        )
    except KeyboardInterrupt:
        logger.warning("Interrupted by user → exiting…")
        sys.exit(130)


if __name__ == "__main__":
    main()
