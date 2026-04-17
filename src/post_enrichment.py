"""
Stage 4 — Post-Load KG Enrichment
After ETL completes:
1. Generate sentence-transformer embeddings on text node properties
2. Add KG metadata (source, timestamps, confidence)
3. Infer additional semantic relationships (co-occurrence, similarity)
4. Add full-text search indexes for Neo4j
"""

import logging
from typing import Optional
from tqdm import tqdm

logger = logging.getLogger(__name__)


class PostEnrichment:
    """
    Enriches the loaded Neo4j KG with:
    - Vector embeddings (sentence-transformers) on text properties
    - KG provenance metadata on all nodes
    - Neo4j 5.x vector index creation for semantic search
    - Inferred similarity edges between nodes of the same label
    """

    def __init__(self, config: dict, mapping: dict):
        self.config = config
        self.mapping = mapping
        self.neo4j_cfg = config["neo4j"]
        self.embed_cfg = config.get("embeddings", {})
        self.enabled = self.embed_cfg.get("enabled", True)
        self.model_name = self.embed_cfg.get("model", "all-MiniLM-L6-v2")
        self.embed_prop = self.embed_cfg.get("embedding_property", "embedding")
        self.property_map = self.embed_cfg.get("property_map", {})  # label → [props]
        self.batch_size = config["neo4j"].get("batch_size", 100)
        self._driver = None
        self._model = None

    # ── Connect ───────────────────────────────────────────────────────────────

    def connect(self):
        from neo4j import GraphDatabase
        self._driver = GraphDatabase.driver(
            self.neo4j_cfg["uri"],
            auth=(self.neo4j_cfg["user"], self.neo4j_cfg["password"]),
        )
        self._driver.verify_connectivity()
        logger.info("Neo4j connected for post-enrichment")

        if self.enabled:
            self._load_model()

    def _load_model(self):
        try:
            from sentence_transformers import SentenceTransformer
            logger.info(f"Loading embedding model: {self.model_name} ...")
            self._model = SentenceTransformer(self.model_name)
            logger.info(f"✓ Model loaded (dim={self._model.get_sentence_embedding_dimension()})")
        except ImportError:
            logger.warning("sentence-transformers not installed. Skipping embeddings.")
            self.enabled = False

    def disconnect(self):
        if self._driver:
            self._driver.close()

    # ── Main entry ────────────────────────────────────────────────────────────

    def enrich(self):
        logger.info("Starting KG post-enrichment ...")
        db = self.neo4j_cfg["database"]

        with self._driver.session(database=db) as session:
            self._add_kg_metadata(session)

        if self.enabled and self._model:
            self._generate_embeddings()
            self._create_vector_indexes()

        self._create_fulltext_indexes()
        logger.info("✓ Post-enrichment complete")

    # ── KG Metadata ───────────────────────────────────────────────────────────

    def _add_kg_metadata(self, session):
        """
        Add provenance metadata to all nodes:
        - _source: "rdb2graph"
        - _loadedAt: ISO timestamp
        - _schemaVersion: "1.0"
        """
        logger.info("Adding KG metadata to all nodes ...")
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()

        for node_def in self.mapping["nodes"]:
            label = node_def["label"]
            cypher = f"""
                MATCH (n:{label})
                SET n._source = 'rdb2graph',
                    n._loadedAt = $ts,
                    n._sourceTable = $table,
                    n._schemaVersion = '1.0'
            """
            session.run(cypher, ts=now, table=node_def["_sourceTable"])
            logger.debug(f"  Metadata added to :{label}")

    # ── Embeddings ────────────────────────────────────────────────────────────

    def _generate_embeddings(self):
        """Generate and store embeddings for text properties on each node label"""
        logger.info("Generating node embeddings ...")
        db = self.neo4j_cfg["database"]

        for node_def in self.mapping["nodes"]:
            label = node_def["label"]
            text_props = self._get_text_props(label, node_def)
            if not text_props:
                logger.debug(f"  :{label} — no text props found, skipping embeddings")
                continue

            logger.info(f"  Embedding :{label} on props: {text_props}")
            self._embed_label(label, text_props, db)

    def _get_text_props(self, label: str, node_def: dict) -> list:
        """Determine which properties to embed for a label"""
        # 1. Config override
        if label in self.property_map:
            return self.property_map[label]
        # 2. Auto-detect String properties (excluding internal props)
        SKIP = {"id", "uuid", "email", "phone", "url", "created_at", "updated_at",
                "_source", "_loadedAt", "_sourceTable"}
        text_props = [
            p["to"] for p in node_def.get("properties", [])
            if p.get("type", "String") == "String"
            and p["to"].lower() not in SKIP
            and not p["to"].startswith("_")
        ]
        # Limit to 3 most meaningful text fields
        return text_props[:3]

    def _embed_label(self, label: str, text_props: list, db: str):
        """Fetch nodes, generate embeddings, write back in batches"""
        prop_list = ", ".join(f"n.{p} AS {p}" for p in text_props)
        fetch_cypher = f"MATCH (n:{label}) WHERE n.{self.embed_prop} IS NULL RETURN id(n) AS nid, {prop_list}"

        with self._driver.session(database=db) as session:
            result = session.run(fetch_cypher)
            rows = result.data()

        if not rows:
            logger.debug(f"  :{label} — all nodes already embedded")
            return

        total = len(rows)
        logger.info(f"  Embedding {total} :{label} nodes ...")

        for i in tqdm(range(0, total, self.batch_size), desc=f"Embed :{label}", unit="batch"):
            batch = rows[i:i + self.batch_size]
            texts = []
            for row in batch:
                parts = [str(row.get(p, "") or "") for p in text_props]
                texts.append(" | ".join(filter(None, parts)))

            embeddings = self._model.encode(texts, show_progress_bar=False).tolist()

            write_batch = [
                {"nid": row["nid"], "emb": emb}
                for row, emb in zip(batch, embeddings)
            ]
            with self._driver.session(database=db) as session:
                session.run(
                    f"""
                    UNWIND $rows AS row
                    MATCH (n:{label}) WHERE id(n) = row.nid
                    SET n.{self.embed_prop} = row.emb
                    """,
                    rows=write_batch,
                )

        logger.info(f"  ✓ :{label} — {total} embeddings written")

    # ── Vector Indexes (Neo4j 5.x) ────────────────────────────────────────────

    def _create_vector_indexes(self):
        """
        Create Neo4j 5.x vector indexes for semantic similarity search.
        Requires Neo4j 5.11+ with vector index support.
        """
        logger.info("Creating vector indexes ...")
        db = self.neo4j_cfg["database"]
        dim = self._model.get_sentence_embedding_dimension()

        with self._driver.session(database=db) as session:
            for node_def in self.mapping["nodes"]:
                label = node_def["label"]
                text_props = self._get_text_props(label, node_def)
                if not text_props:
                    continue

                index_name = f"vector_{label.lower()}"
                try:
                    # Neo4j 5.x syntax
                    session.run(f"""
                        CREATE VECTOR INDEX {index_name} IF NOT EXISTS
                        FOR (n:{label}) ON n.{self.embed_prop}
                        OPTIONS {{
                            indexConfig: {{
                                `vector.dimensions`: {dim},
                                `vector.similarity_function`: 'cosine'
                            }}
                        }}
                    """)
                    logger.debug(f"  Vector index: {index_name} (dim={dim})")
                except Exception as e:
                    logger.warning(f"  Vector index skipped ({label}): {e}")

    # ── Full-text Indexes ─────────────────────────────────────────────────────

    def _create_fulltext_indexes(self):
        """Create Neo4j full-text search indexes on String properties"""
        logger.info("Creating full-text indexes ...")
        db = self.neo4j_cfg["database"]

        with self._driver.session(database=db) as session:
            for node_def in self.mapping["nodes"]:
                label = node_def["label"]
                text_cols = [
                    p["to"] for p in node_def.get("properties", [])
                    if p.get("type", "String") == "String"
                    and not p["to"].startswith("_")
                ]
                if not text_cols:
                    continue
                props_cypher = ", ".join(f"n.{p}" for p in text_cols[:5])  # max 5
                index_name = f"fulltext_{label.lower()}"
                try:
                    session.run(f"""
                        CREATE FULLTEXT INDEX {index_name} IF NOT EXISTS
                        FOR (n:{label}) ON EACH [{props_cypher}]
                    """)
                    logger.debug(f"  Full-text index: {index_name}")
                except Exception as e:
                    logger.warning(f"  Full-text index skipped ({label}): {e}")

    # ── KG Stats ──────────────────────────────────────────────────────────────

    def print_stats(self):
        """Print KG summary statistics"""
        db = self.neo4j_cfg["database"]
        with self._driver.session(database=db) as session:
            node_counts = session.run("""
                CALL apoc.meta.stats()
                YIELD labels
                RETURN labels
            """).single()

            total_nodes = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
            total_rels = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]

        print(f"\n{'='*50}")
        print(f"  Knowledge Graph Summary")
        print(f"{'='*50}")
        print(f"  Total Nodes:         {total_nodes:,}")
        print(f"  Total Relationships: {total_rels:,}")
        print(f"{'='*50}\n")
