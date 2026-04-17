"""
Neo4j Direct Loader
Reads from source relational DB in batches → writes to Neo4j 5.x
Uses UNWIND + MERGE (idempotent) — safe to re-run.
This is both the ETL fallback AND the post-enrichment foundation.
"""

import logging
from typing import Iterator
from tqdm import tqdm

logger = logging.getLogger(__name__)


class Neo4jDirectLoader:
    """
    Batched direct loader: Source DB → Neo4j 5.x
    - MERGE on primary key (idempotent)
    - Batched UNWIND for performance
    - Relationship creation via FK column lookups
    """

    def __init__(self, config: dict, mapping: dict):
        self.config = config
        self.mapping = mapping
        self.neo4j_cfg = config["neo4j"]
        self.source_cfg = config["source_db"]
        self.batch_size = config["source_db"].get("batch_size", 500)
        self.neo4j_batch = config["neo4j"].get("batch_size", 500)
        self._driver = None
        self._src_conn = None

    # ── Connections ───────────────────────────────────────────────────────────

    def connect(self):
        from neo4j import GraphDatabase
        self._driver = GraphDatabase.driver(
            self.neo4j_cfg["uri"],
            auth=(self.neo4j_cfg["user"], self.neo4j_cfg["password"]),
            max_connection_pool_size=self.neo4j_cfg.get("max_connection_pool_size", 50),
        )
        self._driver.verify_connectivity()
        logger.info(f"Neo4j connected: {self.neo4j_cfg['uri']}")
        self._connect_source()

    def _connect_source(self):
        db = self.source_cfg
        if db["type"].lower() == "postgresql":
            import psycopg2
            self._src_conn = psycopg2.connect(
                host=db["host"], port=db["port"], dbname=db["database"],
                user=db["user"], password=db["password"],
            )
        elif db["type"].lower() == "mysql":
            import mysql.connector
            self._src_conn = mysql.connector.connect(
                host=db["host"], port=db["port"], database=db["database"],
                user=db["user"], password=db["password"],
            )
        logger.info(f"Source DB connected: {db['type']} @ {db['host']}")

    def disconnect(self):
        if self._driver:
            self._driver.close()
        if self._src_conn:
            self._src_conn.close()
        logger.info("Connections closed")

    # ── Main load ─────────────────────────────────────────────────────────────

    def load_all(self) -> bool:
        try:
            self._create_constraints()
            self._load_nodes()
            self._load_relationships()
            return True
        except Exception as e:
            logger.error(f"Load failed: {e}", exc_info=True)
            return False

    # ── Constraints ───────────────────────────────────────────────────────────

    def _create_constraints(self):
        """Create uniqueness constraints for all node PKs"""
        logger.info("Creating Neo4j constraints ...")
        db = self.neo4j_cfg["database"]
        with self._driver.session(database=db) as session:
            for node_def in self.mapping["nodes"]:
                label = node_def["label"]
                pk_prop = node_def["primaryKey"]
                constraint_name = f"constraint_{label.lower()}_{pk_prop}"
                cypher = (
                    f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
                    f"FOR (n:{label}) REQUIRE n.{pk_prop} IS UNIQUE"
                )
                try:
                    session.run(cypher)
                    logger.debug(f"  Constraint: {label}.{pk_prop}")
                except Exception as e:
                    logger.warning(f"  Constraint skipped ({label}.{pk_prop}): {e}")

    # ── Nodes ─────────────────────────────────────────────────────────────────

    def _load_nodes(self):
        logger.info(f"Loading nodes ({len(self.mapping['nodes'])} types) ...")
        for node_def in self.mapping["nodes"]:
            self._load_node_type(node_def)

    def _load_node_type(self, node_def: dict):
        label = node_def["label"]
        table = node_def["_sourceTable"]
        pk_prop = node_def["primaryKey"]
        properties = node_def["properties"]
        prop_map = {p["from"]: p["to"] for p in properties}
        pk_col = next((p["from"] for p in properties if p["to"] == pk_prop), pk_prop)

        total = self._count_rows(table)
        logger.info(f"  Loading :{label} from {table} ({total} rows) ...")

        db = self.neo4j_cfg["database"]
        loaded = 0
        with self._driver.session(database=db) as session:
            for batch in tqdm(
                self._fetch_batches(table, properties),
                desc=f":{label}",
                total=(total // self.batch_size) + 1,
                unit="batch",
            ):
                cypher = f"""
                    UNWIND $rows AS row
                    MERGE (n:{label} {{{pk_prop}: row.{pk_prop}}})
                    SET n += row
                """
                session.run(cypher, rows=batch)
                loaded += len(batch)

        logger.info(f"  ✓ :{label} — {loaded} nodes loaded")

    # ── Relationships ─────────────────────────────────────────────────────────

    def _load_relationships(self):
        logger.info(f"Loading relationships ({len(self.mapping['relationships'])} types) ...")
        for rel_def in self.mapping["relationships"]:
            self._load_relationship_type(rel_def)

    def _load_relationship_type(self, rel_def: dict):
        rel_type = rel_def["label"]
        start_label = rel_def["startNode"]
        end_label = rel_def["endNode"]
        source_table = rel_def["_sourceTable"]
        start_col = rel_def["startNodeColumn"]
        end_col = rel_def["endNodeColumn"]

        # Get PK props for start/end nodes
        start_pk = self._get_pk_prop(start_label)
        end_pk = self._get_pk_prop(end_label)

        logger.info(f"  Loading [:{rel_type}] from {source_table}.{start_col} → {end_col} ...")

        # Fetch just the FK columns needed
        col_query = f'SELECT "{start_col}", "{end_col}" FROM "{self._schema()}"."{source_table}" WHERE "{end_col}" IS NOT NULL'
        cursor = self._src_conn.cursor()
        cursor.execute(col_query)

        db = self.neo4j_cfg["database"]
        loaded = 0
        with self._driver.session(database=db) as session:
            batch = []
            for row in cursor:
                batch.append({
                    "startId": row[0],
                    "endId": row[1],
                })
                if len(batch) >= self.neo4j_batch:
                    self._write_rel_batch(session, rel_type, start_label, end_label, start_pk, end_pk, batch)
                    loaded += len(batch)
                    batch = []
            if batch:
                self._write_rel_batch(session, rel_type, start_label, end_label, start_pk, end_pk, batch)
                loaded += len(batch)

        cursor.close()
        logger.info(f"  ✓ [:{rel_type}] — {loaded} relationships loaded")

    def _write_rel_batch(self, session, rel_type, start_label, end_label, start_pk, end_pk, batch):
        cypher = f"""
            UNWIND $rows AS row
            MATCH (a:{start_label} {{{start_pk}: row.startId}})
            MATCH (b:{end_label} {{{end_pk}: row.endId}})
            MERGE (a)-[r:{rel_type}]->(b)
        """
        session.run(cypher, rows=batch)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _schema(self) -> str:
        return self.source_cfg.get("schema", "public")

    def _count_rows(self, table: str) -> int:
        cursor = self._src_conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{self._schema()}"."{table}"')
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def _fetch_batches(self, table: str, properties: list) -> Iterator[list]:
        """Stream rows from source table in batches, mapped to Neo4j property names"""
        col_names = [p["from"] for p in properties]
        prop_names = [p["to"] for p in properties]
        cols_sql = ", ".join(f'"{c}"' for c in col_names)

        cursor = self._src_conn.cursor()
        cursor.execute(f'SELECT {cols_sql} FROM "{self._schema()}"."{table}"')

        batch = []
        for row in cursor:
            record = {}
            for i, val in enumerate(row):
                if val is not None:
                    # Serialize non-primitive types
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    elif not isinstance(val, (str, int, float, bool)):
                        val = str(val)
                    record[prop_names[i]] = val
            batch.append(record)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
        cursor.close()

    def _get_pk_prop(self, label: str) -> str:
        """Look up the PK property name for a given node label"""
        for node_def in self.mapping["nodes"]:
            if node_def["label"] == label:
                return node_def["primaryKey"]
        return "id"  # fallback
