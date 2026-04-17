"""
Stage 2 — Mapping Builder
Builds the enriched mapping.json that neo4j-etl-tool consumes.
Your ER semantic model enriches auto-generated FK names with real relationship labels,
cardinality, direction, and node label overrides.
"""

import json
import logging
import re
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── neo4j-etl mapping schema ──────────────────────────────────────────────────
#
# neo4j-etl-tool expects a mapping.json like:
# {
#   "config": { ... jdbc connection ... },
#   "nodes": [
#     { "name": "Customer", "label": "Customer",
#       "mappingFile": "customers.csv",
#       "properties": [ {"from": "id", "to": "customerId", "type": "Long"} ],
#       "primaryKey": "customerId" }
#   ],
#   "relationships": [
#     { "name": "PLACED_BY", "label": "PLACED_BY",
#       "startNode": "Order", "endNode": "Customer",
#       "mappingFile": "orders.csv",
#       "startNodeColumn": "customer_id", "endNodeColumn": "id" }
#   ]
# }
# ─────────────────────────────────────────────────────────────────────────────

# PostgreSQL type → Neo4j/Java type mapping
PG_TYPE_MAP = {
    "INT": "Long", "INT4": "Long", "INT8": "Long", "BIGINT": "Long",
    "INTEGER": "Long", "SMALLINT": "Integer", "SERIAL": "Long", "BIGSERIAL": "Long",
    "FLOAT": "Double", "FLOAT4": "Double", "FLOAT8": "Double",
    "DOUBLE PRECISION": "Double", "NUMERIC": "Double", "DECIMAL": "Double",
    "REAL": "Double",
    "BOOLEAN": "Boolean", "BOOL": "Boolean",
    "VARCHAR": "String", "TEXT": "String", "CHAR": "String", "BPCHAR": "String",
    "UUID": "String",
    "DATE": "LocalDate", "TIMESTAMP": "LocalDateTime",
    "TIMESTAMPTZ": "ZonedDateTime", "TIME": "LocalTime",
    "JSON": "String", "JSONB": "String",
}

# ER cardinality → neo4j-etl relationship direction hint
CARDINALITY_DIRECTION = {
    "one-to-one":   ("startNode", "endNode"),
    "one-to-many":  ("startNode", "endNode"),
    "many-to-one":  ("startNode", "endNode"),
    "many-to-many": ("startNode", "endNode"),
}


def pg_to_neo4j_type(pg_type: str) -> str:
    base = pg_type.upper().split("(")[0].strip()
    return PG_TYPE_MAP.get(base, "String")


def to_camel_case(name: str) -> str:
    """order_id → orderId"""
    parts = re.split(r"[_\s]+", name)
    return parts[0].lower() + "".join(p.capitalize() for p in parts[1:])


def to_pascal_case(name: str) -> str:
    return "".join(w.capitalize() for w in re.split(r"[_\s]+", name))


class MappingBuilder:
    """
    Builds enriched neo4j-etl mapping.json from:
    1. ERModel (semantic labels, cardinality, direction)
    2. Live DB schema (actual column types, constraints)
    3. DB connection config
    """

    def __init__(self, config: dict):
        self.config = config
        self.source_db = config["source_db"]
        self.neo4j_cfg = config["neo4j"]

    def build(self, er_model, db_schema: dict) -> dict:
        """
        er_model: ERModel dataclass (from er_parser)
        db_schema: dict from schema_extractor — {table_name: [col_dicts]}
        Returns: full mapping dict ready for neo4j-etl
        """
        logger.info("Building enriched mapping.json ...")

        mapping = {
            "config": self._build_jdbc_config(),
            "nodes": [],
            "relationships": [],
            "_meta": {
                "generated_by": "rdb2graph",
                "er_tables": len(er_model.tables),
                "er_relationships": len(er_model.relationships),
            }
        }

        # Build node label lookup from ER model
        er_table_map = {t.name: t for t in er_model.tables}
        er_rel_index = self._index_relationships(er_model.relationships)

        # ── Nodes ──────────────────────────────────────────────────────────
        for tbl_name, columns in db_schema.items():
            er_table = er_table_map.get(tbl_name)
            label = er_table.node_label if er_table else to_pascal_case(tbl_name)
            pk = self._find_pk(columns, er_table)

            node = {
                "name": tbl_name,
                "label": label,
                "mappingFile": f"{tbl_name}.csv",
                "primaryKey": to_camel_case(pk),
                "properties": self._build_properties(columns, er_table),
                "_sourceTable": tbl_name,
            }
            mapping["nodes"].append(node)
            logger.debug(f"  Node: {tbl_name} → :{label} (PK: {pk})")

        # ── Relationships ──────────────────────────────────────────────────
        # From ER model (semantic)
        rel_names_used = set()
        for er_rel in er_model.relationships:
            if not er_rel.from_table or not er_rel.to_table:
                logger.warning(f"  Skipping incomplete rel: {er_rel.name}")
                continue

            from_label = er_table_map.get(er_rel.from_table, None)
            to_label = er_table_map.get(er_rel.to_table, None)
            rel_type = er_rel.semantic_label or f"HAS_{to_pascal_case(er_rel.to_table).upper()}"
            
            # Deduplicate
            rel_key = f"{er_rel.from_table}_{er_rel.from_column}_{er_rel.to_table}"
            if rel_key in rel_names_used:
                continue
            rel_names_used.add(rel_key)

            rel = {
                "name": rel_type,
                "label": rel_type,
                "startNode": from_label.node_label if from_label else to_pascal_case(er_rel.from_table),
                "endNode": to_label.node_label if to_label else to_pascal_case(er_rel.to_table),
                "mappingFile": f"{er_rel.from_table}.csv",
                "startNodeColumn": er_rel.from_column,
                "endNodeColumn": er_rel.to_column,
                "_sourceTable": er_rel.from_table,
                "_cardinality": er_rel.cardinality,
                "_erRelName": er_rel.name,
            }
            mapping["relationships"].append(rel)
            logger.debug(f"  Rel: (:{rel['startNode']})-[:{rel_type}]->(:{rel['endNode']})")

        # Also catch FK relationships found in live DB schema but not in ER model
        for tbl_name, columns in db_schema.items():
            for col in columns:
                if col.get("foreign_table"):
                    rel_key = f"{tbl_name}_{col['column_name']}_{col['foreign_table']}"
                    if rel_key not in rel_names_used:
                        from_label = to_pascal_case(tbl_name)
                        to_label = to_pascal_case(col["foreign_table"])
                        rel_type = f"BELONGS_TO_{to_label.upper()}"
                        mapping["relationships"].append({
                            "name": rel_type,
                            "label": rel_type,
                            "startNode": from_label,
                            "endNode": to_label,
                            "mappingFile": f"{tbl_name}.csv",
                            "startNodeColumn": col["column_name"],
                            "endNodeColumn": col.get("foreign_column", "id"),
                            "_sourceTable": tbl_name,
                            "_cardinality": "many-to-one",
                            "_erRelName": None,
                            "_inferredFromLiveDB": True,
                        })
                        rel_names_used.add(rel_key)
                        logger.debug(f"  Rel (from live DB): (:{from_label})-[:{rel_type}]->(:{to_label})")

        logger.info(f"Mapping built: {len(mapping['nodes'])} nodes, {len(mapping['relationships'])} relationships")
        return mapping

    def _build_jdbc_config(self) -> dict:
        db = self.source_db
        db_type = db["type"].lower()

        if db_type == "postgresql":
            jdbc_url = f"jdbc:postgresql://{db['host']}:{db['port']}/{db['database']}"
        elif db_type == "mysql":
            jdbc_url = f"jdbc:mysql://{db['host']}:{db['port']}/{db['database']}"
        elif db_type == "mssql":
            jdbc_url = f"jdbc:sqlserver://{db['host']}:{db['port']};databaseName={db['database']}"
        elif db_type == "oracle":
            jdbc_url = f"jdbc:oracle:thin:@{db['host']}:{db['port']}:{db['database']}"
        else:
            jdbc_url = f"jdbc:{db_type}://{db['host']}:{db['port']}/{db['database']}"

        return {
            "jdbcUrl": jdbc_url,
            "username": db["user"],
            "password": db["password"],
            "neo4jUri": self.neo4j_cfg["uri"],
            "neo4jUsername": self.neo4j_cfg["user"],
            "neo4jPassword": self.neo4j_cfg["password"],
            "neo4jDatabase": self.neo4j_cfg["database"],
        }

    def _build_properties(self, columns: list, er_table=None) -> list:
        """Build property mapping list for a node"""
        properties = []
        er_col_map = {}
        if er_table:
            er_col_map = {c.name: c for c in er_table.columns}

        for col in columns:
            col_name = col["column_name"]
            er_col = er_col_map.get(col_name)
            pg_type = col.get("data_type", "VARCHAR").upper()
            neo4j_type = pg_to_neo4j_type(pg_type)

            prop = {
                "from": col_name,
                "to": to_camel_case(col_name),
                "type": neo4j_type,
                "_nullable": col.get("is_nullable", True),
            }
            if er_col and er_col.comment:
                prop["_comment"] = er_col.comment
            properties.append(prop)
        return properties

    def _find_pk(self, columns: list, er_table=None) -> str:
        """Determine primary key column"""
        if er_table and er_table.primary_keys:
            return er_table.primary_keys[0]
        for col in columns:
            if col.get("is_primary_key"):
                return col["column_name"]
        # Fallback: look for 'id' column
        for col in columns:
            if col["column_name"].lower() in ("id", "uuid"):
                return col["column_name"]
        return columns[0]["column_name"] if columns else "id"

    def _index_relationships(self, relationships: list) -> dict:
        """Index ER relationships by (from_table, to_table)"""
        idx = {}
        for rel in relationships:
            key = (rel.from_table, rel.to_table)
            idx.setdefault(key, []).append(rel)
        return idx

    @staticmethod
    def save(mapping: dict, path: str = "mappings/enriched_mapping.json"):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(mapping, indent=2))
        logger.info(f"✓ Enriched mapping saved → {path}")

    @staticmethod
    def load(path: str) -> dict:
        return json.loads(Path(path).read_text())


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import yaml
    logging.basicConfig(level=logging.DEBUG)

    cfg = yaml.safe_load(open(sys.argv[1] if len(sys.argv) > 1 else "config.yaml"))
    er_model_json = json.loads(Path("mappings/er_model.json").read_text())

    # Reconstruct lightweight er_model object
    class _M:
        pass
    er_model = _M()
    
    class _T:
        def __init__(self, d):
            self.__dict__.update(d)
            self.columns = [_T(c) for c in d.get("columns", [])]
            self.primary_keys = d.get("primary_keys", [])
    
    class _R:
        def __init__(self, d):
            self.__dict__.update(d)
    
    er_model.tables = [_T(t) for t in er_model_json["tables"]]
    er_model.relationships = [_R(r) for r in er_model_json["relationships"]]

    # Mock DB schema for standalone testing
    db_schema = {
        t.name: [{"column_name": c.name, "data_type": c.data_type,
                  "is_primary_key": c.is_primary_key, "is_nullable": c.is_nullable,
                  "foreign_table": None, "foreign_column": None}
                 for c in t.columns]
        for t in er_model.tables
    }

    builder = MappingBuilder(cfg)
    mapping = builder.build(er_model, db_schema)
    MappingBuilder.save(mapping)
    print(f"✓ Mapping: {len(mapping['nodes'])} nodes, {len(mapping['relationships'])} rels")
