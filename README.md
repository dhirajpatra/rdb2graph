<div align="center">

```
  ____  ____  ____ ____   ____  ____  ____  ____  _  _
 |  _ \|  _ \| __ )___ \ / ___|  _ \|  _ \|  _ \| || |
 | |_) | | | |  _ \ __) | |  _| |_) | |_) | |_) | || |_
 |  _ <| |_| | |_) / __/| |_| |  _ <|  __/|  __/|__   _|
 |_| \_\____/|____/_____|\____|_| \_\_|   |_|      |_|
```

**Relational Database + ER Diagram → Neo4j 5.x Knowledge Graph**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![Neo4j 5.x](https://img.shields.io/badge/neo4j-5.x-green.svg)](https://neo4j.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Contributions Welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg)](CONTRIBUTING.md)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/dhirajpatra/rdb2graph/pulls)

*Convert any relational database into a semantically-enriched Neo4j knowledge graph — with proper relationship labels, vector embeddings, and full-text search. Powered by your own ER diagram.*

[Quick Start](#quick-start) · [Architecture](#architecture) · [Plugin System](#plugin-system) · [Contributing](#contributing) · [Roadmap](#roadmap)

</div>

---

## What is rdb2graph?

Most tools that migrate relational data to Neo4j produce a flat, mechanical graph — every foreign key becomes a generic `HAS_ID` relationship and every table becomes a node with no semantic meaning. The result is a graph database that doesn't actually think like a graph.

**rdb2graph is different.** It uses your ER diagram as a semantic layer — reading relationship names, cardinality, and entity meanings from how *you* designed your schema — and uses that understanding to produce a knowledge graph that accurately reflects your domain model.

```
Before (generic ETL):                After (rdb2graph):

(Order)-[:HAS_CUSTOMER_ID]           (Order)-[:PLACED_BY]->(Customer)
  ->(Customer)                       (Order)-[:CONTAINS]->(Product)
(OrderItem)-[:HAS_ORDER_ID]          (Employee)-[:ASSIGNED_TO]->(Order)
  ->(Order)                          (Product)-[:BELONGS_TO]->(Category)
```

Beyond migration, rdb2graph enriches the graph with **vector embeddings** (for semantic/similarity search), **full-text indexes**, and **KG provenance metadata** — ready for RAG pipelines, graph analytics, and LLM-powered applications.

Also I have a plan to make a multi agents wrapper onver this application. So that we can make this application fully automated with Agentic AI. Find out more about it in [future-multi-agents-readme.md](future-multi-agents-readme.md)


---

## Features

- **Semantic relationship mapping** — ER diagram relationship names become Cypher relationship types
- **MySQL Workbench `.mwb` parsing** — extracts entities, relationships, cardinality, and direction using stdlib only (no Java required for parsing)
- **SQL DDL parsing** — `CREATE TABLE` + `FOREIGN KEY` as fallback
- **PostgreSQL schema introspection** — full column types, PKs, FKs, unique constraints, nullability
- **neo4j-etl-tool integration** — uses the official Neo4j ETL tool (JAR or Docker) when available, with a full Python fallback that always works
- **Batched UNWIND + MERGE loading** — idempotent, safe to re-run, configurable batch sizes
- **Sentence-transformer embeddings** — auto-detected text properties get vector embeddings stored as node properties
- **Neo4j 5.x vector indexes** — created automatically for cosine similarity search
- **Full-text indexes** — on all string properties, per label
- **KG provenance metadata** — `_source`, `_loadedAt`, `_sourceTable` on every node
- **Stage-by-stage control** — run, skip, or re-run individual pipeline stages
- **Extensible plugin architecture** — add new database connectors and ER diagram parsers without touching core code

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          INPUTS                                      │
│                                                                      │
│   ER Diagram (.mwb / .sql / ...)     Source DB (PostgreSQL / ...)   │
└──────────────────┬──────────────────────────────┬───────────────────┘
                   │                              │
          ┌────────▼─────────┐          ┌─────────▼────────┐
          │  STAGE 1         │          │  STAGE 2a        │
          │  ER Parser       │          │  Schema          │
          │  Plugin          │          │  Extractor       │
          │                  │          │  Plugin          │
          │  .mwb → ERModel  │          │                  │
          │  (entities,      │          │  tables, cols,   │
          │  relationships,  │          │  types, PKs,     │
          │  cardinality,    │          │  FKs, nulls      │
          │  direction,      │          │                  │
          │  semantic labels)│          └─────────┬────────┘
          └────────┬─────────┘                    │
                   │                              │
                   └──────────────┬───────────────┘
                                  │
                         ┌────────▼────────┐
                         │  STAGE 2b       │
                         │  Mapping        │
                         │  Builder        │
                         │                 │
                         │  Merges ER      │
                         │  semantics +    │
                         │  live schema    │
                         │  →              │
                         │  enriched_      │
                         │  mapping.json   │
                         └────────┬────────┘
                                  │
                         ┌────────▼────────┐
                         │  STAGE 3        │
                         │  ETL Runner     │
                         │                 │
                         │  neo4j-etl JAR ─┼──► (if available)
                         │  Docker mode   ─┼──► (if configured)
                         │  Python loader ─┼──► (always works)
                         └────────┬────────┘
                                  │
                         ┌────────▼────────┐
                         │  STAGE 4        │
                         │  Post-Load      │
                         │  Enrichment     │
                         │                 │
                         │  Embeddings ────┼──► sentence-transformers
                         │  Vector index ──┼──► Neo4j 5.x
                         │  Fulltext idx ──┼──► Neo4j fulltext
                         │  KG metadata ───┼──► provenance props
                         └────────┬────────┘
                                  │
                         ┌────────▼────────┐
                         │   Neo4j 5.x     │
                         │  Knowledge      │
                         │    Graph        │
                         └─────────────────┘
```

---

## Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL (source database)
- Neo4j 5.x (target — [Neo4j Desktop](https://neo4j.com/download/) or Docker)
- Your MySQL Workbench `.mwb` file

### 1. Clone and install

```bash
git clone https://github.com/dhirajpatra/rdb2graph.git
cd rdb2graph

# Install dependencies
./ingest.sh --install
# or
pip install -r requirements.txt
```

### 2. Configure

Edit `config.yaml`:

```yaml
er_diagram:
  path: "./my_schema.mwb"       # Path to your .mwb file
  format: "mwb"                  # mwb | ddl

source_db:
  type: "postgresql"
  host: "localhost"
  port: 5432
  database: "mydb"
  user: "postgres"
  password: "secret"
  schema: "public"

neo4j:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "neo4j_password"
  database: "neo4j"

embeddings:
  enabled: true
  model: "all-MiniLM-L6-v2"
```

### 3. Validate all connections

```bash
./ingest.sh --validate
```

### 4. Run the full pipeline

```bash
./ingest.sh
```

### 5. Explore your knowledge graph

Open Neo4j Browser at `http://localhost:7474`:

```cypher
-- See the whole graph (small datasets)
MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 50

-- Node counts by label
MATCH (n) RETURN labels(n) AS label, count(n) AS count ORDER BY count DESC

-- Semantic similarity search (requires embeddings)
CALL db.index.vector.queryNodes('vector_customer', 10, $queryEmbedding)
YIELD node, score RETURN node.name, score

-- Full-text search
CALL db.index.fulltext.queryNodes('fulltext_product', 'wireless headphones')
YIELD node RETURN node.name, node.description
```

---

## Pipeline Control

```bash
# Full pipeline (default)
./ingest.sh

# Custom config
./ingest.sh --config production.yaml

# Single stage — useful for testing
./ingest.sh --only er_parse         # Test your .mwb parsing
./ingest.sh --only mapping_build    # Rebuild mapping without reloading
./ingest.sh --only etl_run          # Re-run load only
./ingest.sh --only post_enrich      # Re-generate embeddings only

# Skip a stage
./ingest.sh --skip post_enrich      # Load without embeddings (faster)
./ingest.sh --skip etl_run          # Dry run — build mapping only

# Validate connections without running
./ingest.sh --validate
```

Or directly via Python:

```bash
cd src
python main.py -c ../config.yaml
python main.py --stages er_parse mapping_build
python main.py --skip post_enrich
```

---

## neo4j-etl-tool Integration

rdb2graph integrates with the official [neo4j-etl-tool](https://github.com/neo4j-contrib/neo4j-etl-components) for enterprise-grade bulk loading. It auto-selects the best available strategy:

| Strategy | When used | Setup required |
|----------|-----------|----------------|
| **neo4j-etl JAR** | JAR found at configured path | Download JAR, set path in config |
| **neo4j-etl Docker** | `use_docker: true` in config | Docker installed |
| **Python direct loader** | Default fallback — always works | None |

The key innovation: rdb2graph feeds neo4j-etl an **enriched `mapping.json`** — your ER diagram's semantic relationship labels are injected into the mapping before the ETL tool runs, so the loaded graph has meaningful relationship types instead of raw FK-derived names.

**Using the JAR:**
```bash
# Download from https://github.com/neo4j-contrib/neo4j-etl-components/releases
mkdir -p neo4j-etl && cp neo4j-etl-tool.jar neo4j-etl/neo4j-etl.jar
```

**Using Docker:**
```yaml
neo4j_etl:
  use_docker: true
  docker_image: "neo4j/neo4j-etl-tool:latest"
```

---

## Project Structure

```
rdb2graph/
├── config.yaml                   # All configuration — start here
├── ingest.sh                     # Master shell orchestrator
├── requirements.txt
├── README.md
│
├── src/
│   ├── main.py                   # Pipeline CLI & orchestrator
│   ├── er_parser.py              # Stage 1: ER diagram parsers + factory
│   ├── schema_extractor.py       # Stage 2a: Live DB schema extraction
│   ├── mapping_builder.py        # Stage 2b: ER + schema → mapping.json
│   ├── etl_runner.py             # Stage 3: ETL execution strategies
│   ├── neo4j_loader.py           # Direct Python → Neo4j batched loader
│   └── post_enrichment.py        # Stage 4: Embeddings + KG enrichment
│
├── plugins/                      # ← Community plugin directory
│   ├── db/                       # Database connector plugins
│   │   ├── mysql/
│   │   ├── mssql/
│   │   ├── oracle/
│   │   └── sqlite/
│   └── er/                       # ER diagram parser plugins
│       ├── dbdiagram/
│       ├── lucidchart/
│       ├── drawio/
│       └── image_llm/
│
├── mappings/                     # Generated at runtime (gitignored)
├── logs/                         # Pipeline run logs (gitignored)
└── neo4j-etl/                    # Place neo4j-etl.jar here (gitignored)
```

---

## Plugin System

rdb2graph is built around a **two-axis plugin model**. New source databases and new ER diagram formats can be added as self-contained plugins without modifying any core code. The factory pattern auto-discovers plugins in the `plugins/` directory.

---

## Contributing

We warmly welcome contributions of all kinds — and we especially want to grow the ecosystem of **database connector plugins** and **ER diagram parser plugins**. The plugin interfaces are intentionally simple so you can have a working plugin running in under an hour.

### Plugin Support Matrix

#### Database Connectors

| Database | Status | Plugin location |
|----------|--------|-----------------|
| **PostgreSQL** | ✅ Built-in | `src/schema_extractor.py` |
| MySQL | 🔜 **Contribution wanted** | `plugins/db/mysql/` |
| Microsoft SQL Server | 🔜 **Contribution wanted** | `plugins/db/mssql/` |
| Oracle Database | 🔜 **Contribution wanted** | `plugins/db/oracle/` |
| SQLite | 🔜 **Contribution wanted** | `plugins/db/sqlite/` |
| MariaDB | 🔜 **Contribution wanted** | `plugins/db/mariadb/` |
| Amazon Redshift | 🔜 **Contribution wanted** | `plugins/db/redshift/` |
| Google BigQuery | 🔜 **Contribution wanted** | `plugins/db/bigquery/` |
| Snowflake | 🔜 **Contribution wanted** | `plugins/db/snowflake/` |
| IBM Db2 | 🔜 **Contribution wanted** | `plugins/db/db2/` |
| CockroachDB | 🔜 **Contribution wanted** | `plugins/db/cockroachdb/` |
| Azure SQL | 🔜 **Contribution wanted** | `plugins/db/azure_sql/` |

#### ER Diagram Parsers

| Format / Tool | Status | Plugin location |
|---------------|--------|-----------------|
| **MySQL Workbench `.mwb`** | ✅ Built-in | `src/er_parser.py` |
| **SQL DDL `.sql`** | ✅ Built-in | `src/er_parser.py` |
| dbdiagram.io JSON export | 🔜 **Contribution wanted** | `plugins/er/dbdiagram/` |
| Lucidchart `.xml` / `.vsdx` | 🔜 **Contribution wanted** | `plugins/er/lucidchart/` |
| draw.io / diagrams.net `.drawio` | 🔜 **Contribution wanted** | `plugins/er/drawio/` |
| ERwin `.erwin` / `.xml` | 🔜 **Contribution wanted** | `plugins/er/erwin/` |
| DBeaver ERD export | 🔜 **Contribution wanted** | `plugins/er/dbeaver/` |
| PlantUML `@startuml` | 🔜 **Contribution wanted** | `plugins/er/plantuml/` |
| Mermaid `erDiagram` | 🔜 **Contribution wanted** | `plugins/er/mermaid/` |
| JSON Schema | 🔜 **Contribution wanted** | `plugins/er/json_schema/` |
| PNG / JPG image (LLM vision) | 🔜 **Contribution wanted** | `plugins/er/image_llm/` |
| Vertabelo XML | 🔜 **Contribution wanted** | `plugins/er/vertabelo/` |
| Oracle SQL Developer `.pdm` | 🔜 **Contribution wanted** | `plugins/er/oracle_sqldeveloper/` |

---

### Writing a Database Connector Plugin

A DB connector plugin implements one class inheriting from `SchemaExtractorBase`.

**1. Create the plugin directory:**

```
plugins/db/mysql/
├── __init__.py
├── extractor.py       ← your implementation
├── requirements.txt   ← e.g. mysql-connector-python>=8.0.0
├── README.md          ← supported versions, limitations, sample config
└── tests/
    └── test_mysql.py
```

**2. Implement the interface in `extractor.py`:**

```python
from src.schema_extractor import SchemaExtractorBase

class MySQLExtractor(SchemaExtractorBase):
    """
    MySQL database connector plugin for rdb2graph.
    Extracts tables, columns, primary keys, foreign keys.
    """

    PLUGIN_NAME = "mysql"           # matches config: source_db.type
    REQUIRED_PACKAGES = ["mysql-connector-python"]

    def connect(self):
        import mysql.connector
        self._conn = mysql.connector.connect(
            host=self.config["host"],
            port=self.config["port"],
            database=self.config["database"],
            user=self.config["user"],
            password=self.config["password"],
        )

    def extract(self) -> dict:
        """
        Must return:
        {
            "table_name": [
                {
                    "column_name": str,
                    "data_type": str,           # uppercase e.g. "VARCHAR"
                    "is_primary_key": bool,
                    "is_nullable": bool,
                    "is_unique": bool,
                    "default_value": str | None,
                    "ordinal_position": int,
                    "foreign_table": str | None,
                    "foreign_column": str | None,
                    "fk_constraint_name": str | None,
                }
            ]
        }
        """
        # ... your implementation ...
        pass

    def disconnect(self):
        if self._conn:
            self._conn.close()
```

**3. No registration needed** — the factory auto-discovers plugins in `plugins/db/` by matching `PLUGIN_NAME` to `config.source_db.type`.

**4. Test:**

```bash
pytest plugins/db/mysql/tests/ -v
```

**5. Use it:**

```yaml
# config.yaml
source_db:
  type: "mysql"      # ← matches PLUGIN_NAME
  host: "localhost"
  port: 3306
  database: "mydb"
  user: "root"
  password: "secret"
```

---

### Writing an ER Diagram Parser Plugin

An ER parser plugin implements one class inheriting from `ERParserBase`.

**1. Create the plugin directory:**

```
plugins/er/drawio/
├── __init__.py
├── parser.py          ← your implementation
├── requirements.txt   ← e.g. lxml>=4.9.0
├── README.md
├── sample.drawio      ← sample file for CI testing
└── tests/
    └── test_drawio.py
```

**2. Implement the interface in `parser.py`:**

```python
from src.er_parser import ERParserBase, ERModel, ERTable, ERColumn, ERRelationship

class DrawIOParser(ERParserBase):
    """
    draw.io / diagrams.net ER diagram parser plugin for rdb2graph.
    """

    PLUGIN_NAME = "drawio"          # matches config: er_diagram.format
    FILE_EXTENSIONS = [".drawio", ".xml"]
    REQUIRED_PACKAGES = ["lxml"]

    def __init__(self, path: str):
        self.path = path

    def parse(self) -> ERModel:
        """
        Must return an ERModel containing:

        ERModel.tables: list[ERTable]
          ERTable:
            name          str          e.g. "customers"
            node_label    str          e.g. "Customer"  (PascalCase)
            columns       list[ERColumn]
            primary_keys  list[str]    column names
            comment       str | None

          ERColumn:
            name           str
            data_type      str          e.g. "VARCHAR", "INTEGER"
            is_primary_key bool
            is_foreign_key bool
            is_nullable    bool
            is_unique      bool
            default_value  str | None
            comment        str | None

        ERModel.relationships: list[ERRelationship]
          ERRelationship:
            name           str          raw FK/rel name from diagram
            from_table     str
            to_table       str
            from_column    str
            to_column      str
            cardinality    str          "one-to-one" | "one-to-many" |
                                        "many-to-one" | "many-to-many"
            semantic_label str          Cypher rel type e.g. "PLACED_BY"
            direction      str          "OUTGOING" | "INCOMING" | "UNDIRECTED"
        """
        model = ERModel(source_format="drawio", source_path=self.path)
        # ... your parsing logic ...
        return model
```

**3. Test with the included sample:**

```yaml
# config.yaml for testing
er_diagram:
  path: "./plugins/er/drawio/sample.drawio"
  format: "drawio"
```

```bash
python src/er_parser.py config.yaml   # runs Stage 1 only
pytest plugins/er/drawio/tests/ -v
```

---

### Plugin Contribution Checklist

Before opening a pull request, please confirm:

- [ ] Plugin lives in `plugins/db/<name>/` or `plugins/er/<name>/`
- [ ] Class inherits from `SchemaExtractorBase` or `ERParserBase`
- [ ] `PLUGIN_NAME` is set and matches the config `type`/`format` value
- [ ] `requirements.txt` lists only necessary packages
- [ ] `README.md` in the plugin directory covers: supported versions, known limitations, sample config snippet
- [ ] At least one test in `tests/` using a mock connection or sample file (no live DB/service required for CI to pass)
- [ ] Sample input file included where applicable (anonymized `.mwb`, `.drawio`, `.sql`, etc.)
- [ ] Code formatted with `black` and passes `mypy`
- [ ] No unnecessary changes to core files in `src/`

---

### Good First Issues

If you're looking for somewhere to start to improve, these are high-value and well-scoped:

| Label | Task | Difficulty | Improve |
|-------|------|------------|---------|
| `plugin/db` | MySQL connector | ⭐ Easy | Already done |
| `plugin/db` | SQLite connector | ⭐ Easy | Already done |
| `plugin/er` | Mermaid `erDiagram` parser | ⭐ Easy | Already done |
| `plugin/er` | PlantUML entity parser | ⭐ Easy | Already done |
| `plugin/er` | dbdiagram.io JSON parser | ⭐⭐ Medium | Already done |
| `plugin/er` | draw.io XML parser | ⭐⭐ Medium | Already done |
| `plugin/db` | MSSQL connector | ⭐⭐ Medium | Already done |
| `plugin/db` | Oracle connector | ⭐⭐ Medium | Already done |
| `plugin/er` | Lucidchart `.vsdx` parser | ⭐⭐⭐ Hard | Already done |
| `plugin/er` | PNG/JPG image → LLM vision parser | ⭐⭐⭐ Hard | Already done |
| `plugin/db` | Snowflake connector | ⭐⭐⭐ Hard | Already done |
| `core` | Async parallel batch loading | ⭐⭐⭐ Hard | Already done |

Or if you want to build a multi agent application as wrapper of this application then find out more about it in [future-multi-agents-readme.md](future-multi-agents-readme.md). Hope you are ready to contribute so that many users can get use them for their real use. Thanks

Browse [open issues](https://github.com/dhirajpatra/rdb2graph/issues) and look for `good-first-issue` tags.

---

### General Contribution Guidelines

**For bug fixes and core improvements**, please open an issue first to discuss the approach before submitting a PR.

**Code style:**

```bash
black src/ plugins/           # format
mypy src/                     # type check
pytest tests/ plugins/        # run all tests
```

**Commit format:**

```
plugin/db-mysql: add MySQL connector plugin
plugin/er-drawio: add draw.io XML parser
fix/er-mwb: handle schemas without explicit FK constraints
feat/core: async parallel batch loading
docs: update plugin contribution guide
```

**Pull request process:**

1. Fork → branch (`plugin/db-mysql`) → implement → test → PR to `main`
2. PR title: `[plugin/db] MySQL connector` or `[plugin/er] draw.io parser`
3. Fill in the PR template: what it does, how it was tested, sample output snippet
4. One maintainer approval required to merge

---

## Output: What Gets Created in Neo4j

For each relational table:

| Neo4j artifact | Based on | Example |
|----------------|----------|---------|
| Node label | Table name (PascalCase) | `customers` → `:Customer` |
| Node properties | Column names (camelCase) | `first_name` → `.firstName` |
| Uniqueness constraint | Primary key | `UNIQUE (n.customerId)` |
| KG metadata | Pipeline provenance | `._source`, `._loadedAt`, `._sourceTable` |
| Vector embedding | Text properties | `.embedding` (float[]) |
| Vector index | Embedding property | `vector_customer` |
| Full-text index | String properties | `fulltext_customer` |

For each FK / ER relationship:

| Neo4j artifact | Based on | Example |
|----------------|----------|---------|
| Relationship type | ER diagram name | `PLACED_BY`, `BELONGS_TO`, `AUTHORED_BY` |
| Direction | ER diagram direction | `(Order)→(Customer)` |
| Cardinality metadata | ER diagram cardinality | `._cardinality: "many-to-one"` |

---

## Roadmap

### v1.1 — Plugin ecosystem launch
- [ ] Plugin auto-discovery from `plugins/` directory
- [ ] Formal `SchemaExtractorBase` and `ERParserBase` abstract classes
- [ ] MySQL connector plugin (built-in)
- [ ] Mermaid ERD parser plugin
- [ ] Plugin registry CLI: `python main.py --list-plugins`

### v1.2 — More formats
- [ ] dbdiagram.io JSON parser
- [ ] draw.io XML parser
- [ ] MSSQL and SQLite connectors

### v1.3 — Scale & performance
- [ ] Async/parallel batch loading for tables > 1M rows
- [ ] Incremental sync mode (only load changed rows)
- [ ] Progress persistence (resume interrupted pipelines)

### v2.0 — Intelligence layer
- [ ] LLM vision API parser for PNG/JPG ER diagrams
- [ ] GPT/Claude-powered relationship label inference
- [ ] Automatic domain ontology mapping
- [ ] Graph schema validation post-load

---

## License

MIT License — see [LICENSE](LICENSE).

---

## Author

**Dhiraj Patra**  
Senior Engineering Lead & AI/ML Architect · 28+ years in enterprise technology  
[dhirajpatra.github.io](https://dhirajpatra.github.io) · [LinkedIn](https://linkedin.com/in/dhirajpatra) · [X/Twitter](https://x.com/dhirajpatra) · [Facebook](https://www.facebook.com/dhiraj.patra)

---

<div align="center">

If rdb2graph saves you time, please give it a ⭐ on GitHub and share it.

**Contributions of all kinds are welcome — especially new database connectors and ER diagram parser plugins!**

[Open an Issue](https://github.com/dhirajpatra/rdb2graph/issues/new) · [Start a Discussion](https://github.com/dhirajpatra/rdb2graph/discussions) · [Submit a PR](https://github.com/dhirajpatra/rdb2graph/pulls)

</div>
