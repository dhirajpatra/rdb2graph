"""
Image → LLM Vision ER Diagram Parser Plugin for rdb2graph
Uses an LLM vision API (Claude or OpenAI GPT-4o) to extract ER entities
and relationships from a PNG/JPG/WEBP screenshot of an ER diagram.

Install:  pip install anthropic>=0.25.0
          OR
          pip install openai>=1.0.0  (for GPT-4o backend)

config.yaml:
    er_diagram:
      path: "./schema_screenshot.png"
      format: "image_llm"
      llm_backend: "claude"        # claude | openai  (default: claude)
      llm_model: "claude-opus-4-5" # optional model override
      api_key_env: "ANTHROPIC_API_KEY"   # env var holding the API key
"""
import base64
import json
import logging
import os
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from src.er_parser import ERParserBase, ERModel, ERTable, ERColumn, ERRelationship, to_pascal_case
except ImportError:
    import sys; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../src"))
    from er_parser import ERParserBase, ERModel, ERTable, ERColumn, ERRelationship, to_pascal_case  # type: ignore

EXTRACTION_PROMPT = """
You are an ER diagram analysis expert. Analyze the provided ER diagram image and extract the full schema.

Return ONLY a valid JSON object with this exact structure (no markdown, no explanation):
{
  "tables": [
    {
      "name": "table_name",
      "columns": [
        {
          "name": "col_name",
          "data_type": "VARCHAR",
          "is_primary_key": false,
          "is_foreign_key": false,
          "is_nullable": true,
          "is_unique": false
        }
      ]
    }
  ],
  "relationships": [
    {
      "from_table": "orders",
      "from_column": "customer_id",
      "to_table": "customers",
      "to_column": "id",
      "cardinality": "many-to-one",
      "semantic_label": "PLACED_BY"
    }
  ]
}

Rules:
- data_type must be uppercase: VARCHAR, INTEGER, BIGINT, BOOLEAN, DATE, TIMESTAMP, TEXT, FLOAT, DECIMAL, UUID
- cardinality: one-to-one | one-to-many | many-to-one | many-to-many
- semantic_label: derive a meaningful Cypher relationship type from the FK name or diagram annotation
  e.g. "customer_id" → "PLACED_BY", "author_id" → "AUTHORED_BY", "parent_id" → "HAS_PARENT"
- Include ALL tables and relationships visible in the diagram
"""


class ImageLLMParser(ERParserBase):
    """
    Parses ER diagram images using LLM vision API.
    Supports Claude (Anthropic) and GPT-4o (OpenAI) backends.
    """

    PLUGIN_NAME = "image_llm"
    FILE_EXTENSIONS = [".png", ".jpg", ".jpeg", ".webp", ".gif"]
    REQUIRED_PACKAGES = ["anthropic>=0.25.0"]   # or openai>=1.0.0

    def __init__(self, path: str, config: dict = None):
        self.path = path
        self.config = (config or {}).get("er_diagram", {})
        self.backend = self.config.get("llm_backend", "claude").lower()
        self.api_key_env = self.config.get("api_key_env",
            "ANTHROPIC_API_KEY" if self.backend == "claude" else "OPENAI_API_KEY")
        self.model = self.config.get("llm_model", None)

    def parse(self) -> ERModel:
        logger.info(f"Parsing ER diagram image via LLM ({self.backend}): {self.path}")
        image_b64, media_type = self._load_image()

        if self.backend == "claude":
            raw_json = self._call_claude(image_b64, media_type)
        elif self.backend == "openai":
            raw_json = self._call_openai(image_b64, media_type)
        else:
            raise ValueError(f"Unknown LLM backend: '{self.backend}'. Use 'claude' or 'openai'.")

        return self._build_model(raw_json)

    # ── Image loading ─────────────────────────────────────────────────────────

    def _load_image(self) -> tuple[str, str]:
        ext = Path(self.path).suffix.lower()
        media_map = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                     ".webp": "image/webp", ".gif": "image/gif"}
        media_type = media_map.get(ext, "image/png")
        with open(self.path, "rb") as f:
            b64 = base64.standard_b64encode(f.read()).decode("utf-8")
        logger.debug(f"Image loaded: {self.path} ({media_type}, {len(b64)//1024}KB base64)")
        return b64, media_type

    # ── Claude backend ────────────────────────────────────────────────────────

    def _call_claude(self, image_b64: str, media_type: str) -> dict:
        try:
            import anthropic
        except ImportError:
            raise ImportError("Run: pip install anthropic")
        api_key = os.environ.get(self.api_key_env)
        if not api_key:
            raise EnvironmentError(f"Set env var {self.api_key_env} with your Anthropic API key")
        client = anthropic.Anthropic(api_key=api_key)
        model = self.model or "claude-opus-4-5"
        logger.info(f"Calling Claude ({model}) for image analysis ...")
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": image_b64}},
                    {"type": "text", "text": EXTRACTION_PROMPT},
                ],
            }],
        )
        raw = response.content[0].text.strip()
        return self._parse_json(raw)

    # ── OpenAI backend ────────────────────────────────────────────────────────

    def _call_openai(self, image_b64: str, media_type: str) -> dict:
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("Run: pip install openai")
        api_key = os.environ.get(self.api_key_env)
        if not api_key:
            raise EnvironmentError(f"Set env var {self.api_key_env} with your OpenAI API key")
        client = OpenAI(api_key=api_key)
        model = self.model or "gpt-4o"
        logger.info(f"Calling OpenAI ({model}) for image analysis ...")
        response = client.chat.completions.create(
            model=model,
            max_tokens=4096,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": EXTRACTION_PROMPT},
                    {"type": "image_url", "image_url": {"url": f"data:{media_type};base64,{image_b64}"}},
                ],
            }],
        )
        raw = response.choices[0].message.content.strip()
        return self._parse_json(raw)

    # ── JSON → ERModel ────────────────────────────────────────────────────────

    def _parse_json(self, raw: str) -> dict:
        # Strip markdown fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
        raw = re.sub(r"\s*```$", "", raw, flags=re.MULTILINE)
        try:
            return json.loads(raw.strip())
        except json.JSONDecodeError as e:
            logger.error(f"LLM returned invalid JSON: {e}\nRaw: {raw[:500]}")
            raise

    def _build_model(self, data: dict) -> ERModel:
        model = ERModel(source_format="image_llm", source_path=self.path)
        for t in data.get("tables", []):
            table = ERTable(name=t["name"], node_label=to_pascal_case(t["name"]))
            for c in t.get("columns", []):
                col = ERColumn(
                    name=c.get("name", ""),
                    data_type=c.get("data_type", "VARCHAR"),
                    is_primary_key=c.get("is_primary_key", False),
                    is_foreign_key=c.get("is_foreign_key", False),
                    is_nullable=c.get("is_nullable", True),
                    is_unique=c.get("is_unique", False),
                )
                table.columns.append(col)
                if col.is_primary_key:
                    table.primary_keys.append(col.name)
            model.tables.append(table)
        for r in data.get("relationships", []):
            model.relationships.append(ERRelationship(
                name=f"rel_{r.get('from_table')}_{r.get('from_column')}",
                from_table=r.get("from_table", ""),
                to_table=r.get("to_table", ""),
                from_column=r.get("from_column", "id"),
                to_column=r.get("to_column", "id"),
                cardinality=r.get("cardinality", "many-to-one"),
                semantic_label=r.get("semantic_label", ""),
                direction="OUTGOING",
            ))
        logger.info(f"LLM extracted: {len(model.tables)} tables, {len(model.relationships)} relationships")
        return model
