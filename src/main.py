"""
rdb2graph — Main Pipeline Orchestrator
Relational DB + ER Diagram → Neo4j Knowledge Graph

Stages:
  1. er_parse      Parse .mwb ER diagram → semantic model
  2. mapping_build  Merge ER model + live DB schema → enriched mapping.json
  3. etl_run        Run neo4j-etl-tool (or Python fallback)
  4. post_enrich    Add embeddings, vector indexes, KG metadata
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict
from pathlib import Path

import yaml


# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logging(level: str = "INFO"):
    try:
        import colorlog
        handler = colorlog.StreamHandler()
        handler.setFormatter(colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
            log_colors={
                "DEBUG": "cyan", "INFO": "green",
                "WARNING": "yellow", "ERROR": "red", "CRITICAL": "bold_red",
            }
        ))
        logging.basicConfig(level=getattr(logging, level.upper()), handlers=[handler])
    except ImportError:
        logging.basicConfig(
            level=getattr(logging, level.upper()),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )


logger = logging.getLogger("rdb2graph")


# ── Pipeline ──────────────────────────────────────────────────────────────────

class Pipeline:

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.pipeline_cfg = self.config.get("pipeline", {})
        self.stages = self.pipeline_cfg.get("stages", ["er_parse", "mapping_build", "etl_run", "post_enrich"])
        self.skip = set(self.pipeline_cfg.get("skip_stages", []))
        self.on_error = self.pipeline_cfg.get("on_error", "stop")
        setup_logging(self.pipeline_cfg.get("log_level", "INFO"))

    def _load_config(self, path: str) -> dict:
        with open(path) as f:
            cfg = yaml.safe_load(f)
        logger.debug(f"Config loaded from {path}")
        return cfg

    def run(self):
        logger.info("=" * 60)
        logger.info("  rdb2graph Pipeline Starting")
        logger.info("=" * 60)
        start_time = time.time()

        er_model = None
        db_schema = None
        mapping = None

        # ── Stage 1: ER Parse ────────────────────────────────────────────────
        if "er_parse" in self.stages and "er_parse" not in self.skip:
            er_model = self._stage_er_parse()
            if er_model is None and self.on_error == "stop":
                sys.exit(1)
        else:
            # Load from cache if skipping
            cache = Path("mappings/er_model.json")
            if cache.exists():
                logger.info("Loading cached ER model from mappings/er_model.json")
                er_model = self._load_er_model_cache(cache)

        # ── Stage 2: Mapping Build ───────────────────────────────────────────
        if "mapping_build" in self.stages and "mapping_build" not in self.skip:
            db_schema = self._stage_extract_schema()
            if db_schema is None and self.on_error == "stop":
                sys.exit(1)
            mapping = self._stage_mapping_build(er_model, db_schema)
            if mapping is None and self.on_error == "stop":
                sys.exit(1)
        else:
            cache = Path("mappings/enriched_mapping.json")
            if cache.exists():
                logger.info("Loading cached mapping from mappings/enriched_mapping.json")
                mapping = json.loads(cache.read_text())

        # ── Stage 3: ETL Run ─────────────────────────────────────────────────
        if "etl_run" in self.stages and "etl_run" not in self.skip:
            success = self._stage_etl_run(mapping)
            if not success and self.on_error == "stop":
                sys.exit(1)

        # ── Stage 4: Post Enrich ─────────────────────────────────────────────
        if "post_enrich" in self.stages and "post_enrich" not in self.skip:
            self._stage_post_enrich(mapping)

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"  ✓ Pipeline complete in {elapsed:.1f}s")
        logger.info("=" * 60)

    # ── Stage implementations ─────────────────────────────────────────────────

    def _stage_er_parse(self):
        logger.info("─── Stage 1: ER Diagram Parse ───")
        try:
            from er_parser import ERParserFactory
            model = ERParserFactory.parse(self.config)
            # Cache to disk
            Path("mappings").mkdir(exist_ok=True)
            out = {
                "tables": [asdict(t) for t in model.tables],
                "relationships": [asdict(r) for r in model.relationships],
            }
            Path("mappings/er_model.json").write_text(json.dumps(out, indent=2))
            logger.info(f"✓ ER model: {len(model.tables)} tables, {len(model.relationships)} relationships")
            return model
        except Exception as e:
            logger.error(f"ER parse failed: {e}", exc_info=True)
            return None

    def _stage_extract_schema(self):
        logger.info("─── Stage 2a: Live DB Schema Extraction ───")
        try:
            from schema_extractor import SchemaExtractor
            extractor = SchemaExtractor(self.config)
            extractor.connect()
            schema = extractor.extract()
            extractor.disconnect()
            # Cache
            Path("mappings/db_schema.json").write_text(json.dumps(schema, indent=2, default=str))
            logger.info(f"✓ Schema: {len(schema)} tables extracted")
            return schema
        except Exception as e:
            logger.error(f"Schema extraction failed: {e}", exc_info=True)
            return None

    def _stage_mapping_build(self, er_model, db_schema):
        logger.info("─── Stage 2b: Mapping Build ───")
        try:
            from mapping_builder import MappingBuilder
            builder = MappingBuilder(self.config)
            mapping = builder.build(er_model, db_schema)
            MappingBuilder.save(mapping)
            logger.info(f"✓ Mapping: {len(mapping['nodes'])} nodes, {len(mapping['relationships'])} relationships")
            return mapping
        except Exception as e:
            logger.error(f"Mapping build failed: {e}", exc_info=True)
            return None

    def _stage_etl_run(self, mapping) -> bool:
        logger.info("─── Stage 3: ETL Run ───")
        try:
            from etl_runner import ETLRunner
            runner = ETLRunner(self.config, mapping)
            success = runner.run()
            if success:
                logger.info("✓ ETL complete")
            else:
                logger.error("ETL run failed")
            return success
        except Exception as e:
            logger.error(f"ETL stage failed: {e}", exc_info=True)
            return False

    def _stage_post_enrich(self, mapping):
        logger.info("─── Stage 4: Post-Load KG Enrichment ───")
        try:
            from post_enrichment import PostEnrichment
            enricher = PostEnrichment(self.config, mapping)
            enricher.connect()
            enricher.enrich()
            enricher.print_stats()
            enricher.disconnect()
            logger.info("✓ Post-enrichment complete")
        except Exception as e:
            logger.error(f"Post-enrichment failed: {e}", exc_info=True)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _load_er_model_cache(self, path: Path):
        """Reconstruct a lightweight ER model object from cached JSON"""
        data = json.loads(path.read_text())

        class SimpleObj:
            def __init__(self, d):
                self.__dict__.update(d)

        class Table(SimpleObj):
            def __init__(self, d):
                super().__init__(d)
                self.columns = [SimpleObj(c) for c in d.get("columns", [])]
                self.primary_keys = d.get("primary_keys", [])

        model = type("ERModel", (), {})()
        model.tables = [Table(t) for t in data["tables"]]
        model.relationships = [SimpleObj(r) for r in data["relationships"]]
        return model


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="rdb2graph — Convert Relational DB + ER Diagram to Neo4j Knowledge Graph",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                          # Use default config.yaml
  python main.py -c my_config.yaml        # Custom config
  python main.py --stages er_parse        # Run only Stage 1
  python main.py --skip etl_run           # Skip ETL (dry run)
  python main.py --validate               # Validate config only
        """,
    )
    parser.add_argument("-c", "--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--stages", nargs="+", help="Override stages to run",
                        choices=["er_parse", "mapping_build", "etl_run", "post_enrich"])
    parser.add_argument("--skip", nargs="+", help="Stages to skip",
                        choices=["er_parse", "mapping_build", "etl_run", "post_enrich"])
    parser.add_argument("--validate", action="store_true", help="Validate config and connections only")
    parser.add_argument("--version", action="version", version="rdb2graph 1.0.0")

    args = parser.parse_args()

    pipeline = Pipeline(args.config)

    if args.stages:
        pipeline.stages = args.stages
    if args.skip:
        pipeline.skip = set(args.skip)

    if args.validate:
        _validate(pipeline.config)
        return

    pipeline.run()


def _validate(config: dict):
    """Quick validation of all connections"""
    setup_logging("INFO")
    logger.info("Validating configuration ...")
    ok = True

    # ER file
    er_path = config["er_diagram"]["path"]
    if Path(er_path).exists():
        logger.info(f"✓ ER file found: {er_path}")
    else:
        logger.error(f"✗ ER file not found: {er_path}")
        ok = False

    # Source DB
    try:
        from schema_extractor import SchemaExtractor
        ext = SchemaExtractor(config)
        ext.connect()
        ext.disconnect()
        logger.info("✓ Source DB connection OK")
    except Exception as e:
        logger.error(f"✗ Source DB connection failed: {e}")
        ok = False

    # Neo4j
    try:
        from neo4j import GraphDatabase
        d = GraphDatabase.driver(
            config["neo4j"]["uri"],
            auth=(config["neo4j"]["user"], config["neo4j"]["password"]),
        )
        d.verify_connectivity()
        d.close()
        logger.info("✓ Neo4j connection OK")
    except Exception as e:
        logger.error(f"✗ Neo4j connection failed: {e}")
        ok = False

    if ok:
        logger.info("✓ All validations passed — ready to run")
    else:
        logger.error("✗ Validation failed — fix config before running")
        sys.exit(1)


if __name__ == "__main__":
    # Run from the project root so relative paths work
    import os
    os.chdir(Path(__file__).parent.parent)
    sys.path.insert(0, str(Path(__file__).parent))
    main()
