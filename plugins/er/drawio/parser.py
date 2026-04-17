"""
draw.io / diagrams.net ER Diagram Parser Plugin for rdb2graph
Parses .drawio (XML) files containing ER diagrams built with draw.io.

Install:  No extra packages needed (stdlib xml.etree only).

config.yaml:
    er_diagram:
      path: "./schema.drawio"
      format: "drawio"

Notes:
  - draw.io ER shapes use mxCell with style containing "shape=table" or "shape=mxgraph.erd"
  - This parser handles the most common "Entity" shape style from draw.io's ER template.
  - For non-standard diagrams, override _is_entity_cell() and _is_relation_cell().
"""
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from src.er_parser import ERParserBase, ERModel, ERTable, ERColumn, ERRelationship, to_pascal_case
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../src"))
    from er_parser import ERParserBase, ERModel, ERTable, ERColumn, ERRelationship, to_pascal_case  # type: ignore


class DrawIOParser(ERParserBase):
    """
    Parser for draw.io .drawio XML files with ER diagram shapes.

    draw.io ER shapes:
      - Entity table:  mxCell style contains "shape=table" or value is the table name with child rows
      - Column row:    child mxCell of the table cell
      - Relationship:  mxCell with edge=1 connecting two entity cells
    """

    PLUGIN_NAME = "drawio"
    FILE_EXTENSIONS = [".drawio", ".xml"]
    REQUIRED_PACKAGES = []

    def __init__(self, path: str):
        self.path = path
        self._cell_map: dict = {}   # cell_id → (table_name, col_name or None)

    def parse(self) -> ERModel:
        logger.info(f"Parsing draw.io file: {self.path}")
        tree = ET.parse(self.path)
        root = tree.getroot()
        model = ERModel(source_format="drawio", source_path=self.path)

        cells = list(root.iter("mxCell"))
        logger.debug(f"  Found {len(cells)} mxCell elements")

        # Pass 1: collect entity (table) cells
        entity_cells = {}   # cell_id → table_name
        for cell in cells:
            if self._is_entity_cell(cell):
                name = (cell.get("value") or "").strip()
                if name:
                    entity_cells[cell.get("id")] = name

        # Pass 2: collect column cells (children of entity cells)
        col_cells = {}  # cell_id → (table_name, col_label)
        for cell in cells:
            parent_id = cell.get("parent")
            if parent_id in entity_cells and self._is_column_cell(cell):
                col_cells[cell.get("id")] = (entity_cells[parent_id], cell.get("value", ""))

        # Build ERTable objects
        table_objs: dict[str, ERTable] = {}
        for cell_id, tbl_name in entity_cells.items():
            table = ERTable(name=tbl_name, node_label=to_pascal_case(tbl_name))
            table_objs[tbl_name] = table

        for cell_id, (tbl_name, col_label) in col_cells.items():
            col = self._parse_column_label(col_label)
            if col and tbl_name in table_objs:
                table_objs[tbl_name].columns.append(col)
                if col.is_primary_key:
                    table_objs[tbl_name].primary_keys.append(col.name)

        model.tables = list(table_objs.values())

        # Pass 3: collect edge (relationship) cells
        for cell in cells:
            if cell.get("edge") == "1":
                rel = self._parse_edge(cell, entity_cells)
                if rel:
                    model.relationships.append(rel)

        logger.info(f"draw.io: {len(model.tables)} tables, {len(model.relationships)} relationships")
        return model

    # ── Cell classification ───────────────────────────────────────────────────

    def _is_entity_cell(self, cell: ET.Element) -> bool:
        style = cell.get("style", "")
        value = cell.get("value", "")
        # draw.io ER entity shapes
        return (
            "shape=table" in style
            or "swimlane" in style
            or "shape=mxgraph.erd.entity" in style
            or (cell.get("vertex") == "1" and value and not cell.get("parent", "").isdigit())
        )

    def _is_column_cell(self, cell: ET.Element) -> bool:
        style = cell.get("style", "")
        return cell.get("vertex") == "1" and (
            "tableRow" in style
            or "swimlaneHead" not in style
        )

    # ── Column parsing ────────────────────────────────────────────────────────

    def _parse_column_label(self, label: str) -> Optional[ERColumn]:
        """
        Handles common draw.io column label formats:
          "id (PK) : INTEGER"
          "id INTEGER PK"
          "id"
        """
        label = re.sub(r"<[^>]+>", "", label).strip()   # strip HTML
        if not label:
            return None
        is_pk = bool(re.search(r"\bPK\b", label, re.IGNORECASE))
        is_fk = bool(re.search(r"\bFK\b", label, re.IGNORECASE))
        # Extract type if present after : or space
        type_m = re.search(r"[:\s]\s*([A-Z][A-Z0-9_]+)", label)
        dtype = type_m.group(1).upper() if type_m else "VARCHAR"
        # Extract column name (first word/token, stripping PK/FK annotations)
        name_m = re.match(r"([a-zA-Z_]\w*)", label)
        if not name_m:
            return None
        return ERColumn(
            name=name_m.group(1),
            data_type=dtype,
            is_primary_key=is_pk,
            is_foreign_key=is_fk,
            is_nullable=not is_pk,
        )

    # ── Edge / Relationship parsing ───────────────────────────────────────────

    def _parse_edge(self, cell: ET.Element, entity_cells: dict) -> Optional[ERRelationship]:
        src = cell.get("source")
        tgt = cell.get("target")
        if not src or not tgt:
            return None
        from_table = entity_cells.get(src)
        to_table = entity_cells.get(tgt)
        if not from_table or not to_table:
            return None
        style = cell.get("style", "")
        # Infer cardinality from arrow style
        cardinality = "many-to-one"
        if "ERmany" in style and "ERone" in style:
            cardinality = "many-to-one"
        elif "ERone" in style:
            cardinality = "one-to-one"
        label = to_pascal_case(to_table).upper()
        return ERRelationship(
            name=cell.get("value") or f"rel_{from_table}_{to_table}",
            from_table=from_table,
            to_table=to_table,
            from_column="id",
            to_column="id",
            cardinality=cardinality,
            semantic_label=f"BELONGS_TO_{label}",
            direction="OUTGOING",
        )
