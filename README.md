# DCTerm to CIDOC CRM ETL Pipeline

This project provides an ETL (Extract, Transform, Load) pipeline to convert RDF data from a Fedora repository, specifically using Dublin Core (DC) terms, into the CIDOC Conceptual Reference Model (CRM) and CRMdig (for digital objects).

The transformation is driven by a flexible, YAML-based rules engine, allowing for clear and maintainable mapping from source to target ontologies.

## Features

*   **Fedora Repository Crawler**: Traverses a Fedora 4 repository to harvest RDF data.
*   **Rule-Based Transformation**: Maps source RDF predicates to target CIDOC CRM structures using a `rules.yaml` file.
*   **Chunking & Streaming**: Processes resources in configurable chunks to handle large datasets with bounded memory usage.
*   **Artefact Generation**: For each chunk, it generates three files:
    *   `source-NNN.ttl`: The original RDF data from Fedora for provenance and auditing.
    *   `dataset-NNN.trig`: The transformed triples in CIDOC CRM.
    *   `insert-NNN.rq`: A self-contained SPARQL `INSERT DATA` query for easy ingestion into a triplestore like ResearchSpace.
*   **Handles Binaries**: Intelligently fetches RDF metadata for binary resources (NonRDFSource).
*   **Authentication**: Supports basic authentication for accessing the Fedora repository.

## How it Works

The `etl_pipeline.py` script performs the following steps:

1.  **Connects** to the specified Fedora repository base URL.
2.  **Crawls** the repository starting from a given root path.
3.  **Fetches** RDF data for each resource.
4.  **Applies** the transformation rules defined in `rules.yaml`. Each rule maps a `source_predicate` (like `dcterms:title`) to a `target_pattern` that defines the corresponding CIDOC CRM structure.
5.  **Writes** the output artifacts (`.ttl`, `.trig`, `.rq`) to a specified output directory.

## Usage

To run the ETL pipeline, use the following command structure, providing the necessary arguments.

### Basic Example

This example runs the pipeline, processing up to 100 resources from the specified Fedora instance and path, with a chunk size of 5000.

**Bash:**
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

**PowerShell:**
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

### Arguments

*   `--fedora-base`: The base URL of the Fedora repository.
*   `--root-path`: The starting path within the repository to begin crawling.
*   `--rules-file`: Path to the YAML file containing the mapping rules (default: `rules.yaml`).
*   `--out-dir`: The directory where the output files will be saved (default: `sparql_out`).
*   `--username`: The username for basic authentication.
*   `--password`: The password for basic authentication.
*   `--chunk-size`: The number of resources to process in each chunk (default: 10000).
*   `--max-resources`: The maximum number of resources to process (default: 0, for unlimited).
*   `-v`, `--verbose`: Enable verbose logging.

## The `rules.yaml` File

The core of the transformation logic resides in the `rules.yaml` file. This file defines a list of rules that map predicates from the source RDF to SPARQL patterns for the target CIDOC CRM representation.

Each rule consists of:
*   `id`: A unique identifier for the rule.
*   `source_predicate`: The full URI of the predicate in the source data to be transformed.
*   `target_pattern`: A SPARQL graph pattern that will be instantiated for each triple matching the `source_predicate`. The variables `?s` (subject) and `?o` (object) from the source triple can be used in the pattern.

### Example Rule

This rule maps the Dublin Core title (`dcterms:title`) to a CIDOC CRM `E35_Title`.

```yaml
- id: dcterms_title
  source_predicate: "http://purl.org/dc/terms/title"
  target_pattern: |
    ?s crm:P102_has_title [
        a crm:E35_Title ;
        rdf:value ?o
    ] .
```

## Dependencies

*   Python 3

Python dependencies are listed in the `requirements.txt` file and can be installed using pip:

```bash
pip install -r requirements.txt
```