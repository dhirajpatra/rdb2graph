"""
Stage 3 — neo4j-etl Runner
Invokes neo4j-etl-tool CLI using the enriched mapping.json.
Falls back to direct Python → Neo4j loader if JAR not found.
"""

import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class ETLRunner:
    """
    Orchestrates neo4j-etl-tool execution.
    
    Strategy:
    1. If neo4j-etl JAR/binary exists → use it via subprocess (full ETL power)
    2. If neo4j-etl not available → delegate to Neo4jDirectLoader (pure Python fallback)
    
    neo4j-etl-tool CLI usage:
      neo4j-etl export --rdbms:url <jdbc> --rdbms:user <u> --rdbms:password <p>
                       --destination /tmp/csv --using mapping:from-mapping-file=mapping.json
      neo4j-etl import --destination /tmp/csv --neo4j:url <bolt> ...
    """

    DOWNLOAD_URL = "https://github.com/neo4j-contrib/neo4j-etl-components/releases/latest"

    def __init__(self, config: dict, mapping: dict):
        self.config = config
        self.mapping = mapping
        self.etl_cfg = config.get("neo4j_etl", {})
        self.jar_path = self.etl_cfg.get("jar_path", "./neo4j-etl/neo4j-etl.jar")
        self.csv_dir = self.etl_cfg.get("csv_dir", "/tmp/rdb2graph_csv")
        self.use_docker = self.etl_cfg.get("use_docker", False)
        self.docker_image = self.etl_cfg.get("docker_image", "neo4j/neo4j-etl-tool:latest")

    def run(self) -> bool:
        """Entry point — auto-selects strategy"""
        if self.use_docker:
            return self._run_docker()
        elif Path(self.jar_path).exists():
            return self._run_jar()
        else:
            logger.warning(
                f"neo4j-etl JAR not found at '{self.jar_path}'. "
                f"Download from: {self.DOWNLOAD_URL}\n"
                f"Falling back to direct Python loader."
            )
            return self._run_direct_fallback()

    # ── JAR mode ──────────────────────────────────────────────────────────────

    def _run_jar(self) -> bool:
        mapping_path = "mappings/enriched_mapping.json"
        Path(self.csv_dir).mkdir(parents=True, exist_ok=True)

        db = self.config["source_db"]
        neo4j = self.config["neo4j"]

        db_type = db["type"].lower()
        jdbc_prefix = {
            "postgresql": "postgresql",
            "mysql": "mysql",
            "mssql": "sqlserver",
            "oracle": "oracle:thin",
        }.get(db_type, db_type)
        jdbc_url = f"jdbc:{jdbc_prefix}://{db['host']}:{db['port']}/{db['database']}"

        # Step 1: Export relational DB to CSVs
        export_cmd = [
            "java", "-jar", self.jar_path,
            "export",
            f"--rdbms:url={jdbc_url}",
            f"--rdbms:user={db['user']}",
            f"--rdbms:password={db['password']}",
            f"--destination={self.csv_dir}",
            f"--using=mapping:from-mapping-file={mapping_path}",
        ]
        logger.info("Running neo4j-etl export ...")
        logger.debug(f"CMD: {' '.join(export_cmd)}")
        result = subprocess.run(export_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"neo4j-etl export failed:\n{result.stderr}")
            return False
        logger.info("✓ Export complete")

        # Step 2: Import CSVs into Neo4j
        import_cmd = [
            "java", "-jar", self.jar_path,
            "import",
            f"--destination={self.csv_dir}",
            f"--neo4j:url={neo4j['uri']}",
            f"--neo4j:user={neo4j['user']}",
            f"--neo4j:password={neo4j['password']}",
            f"--neo4j:database={neo4j['database']}",
        ]
        logger.info("Running neo4j-etl import ...")
        result = subprocess.run(import_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"neo4j-etl import failed:\n{result.stderr}")
            return False
        logger.info("✓ Import complete")
        return True

    # ── Docker mode ───────────────────────────────────────────────────────────

    def _run_docker(self) -> bool:
        mapping_path = Path("mappings/enriched_mapping.json").absolute()
        db = self.config["source_db"]
        neo4j = self.config["neo4j"]

        db_type = db["type"].lower()
        jdbc_url = f"jdbc:{db_type}://{db['host']}:{db['port']}/{db['database']}"

        docker_cmd = [
            "docker", "run", "--rm",
            "-v", f"{mapping_path.parent}:/mappings",
            "-v", f"{self.csv_dir}:/csv",
            self.docker_image,
            "export",
            f"--rdbms:url={jdbc_url}",
            f"--rdbms:user={db['user']}",
            f"--rdbms:password={db['password']}",
            "--destination=/csv",
            "--using=mapping:from-mapping-file=/mappings/enriched_mapping.json",
        ]
        logger.info("Running neo4j-etl via Docker ...")
        logger.debug(f"CMD: {' '.join(docker_cmd)}")
        result = subprocess.run(docker_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Docker ETL failed:\n{result.stderr}")
            return False
        logger.info("✓ Docker ETL complete")
        return True

    # ── Direct Python Fallback ─────────────────────────────────────────────────

    def _run_direct_fallback(self) -> bool:
        """
        When neo4j-etl JAR is not available:
        Uses Neo4jDirectLoader to read from source DB and write to Neo4j
        directly using Python + Cypher UNWIND batches.
        """
        logger.info("Using direct Python → Neo4j loader (ETL fallback)")
        from neo4j_loader import Neo4jDirectLoader
        loader = Neo4jDirectLoader(self.config, self.mapping)
        loader.connect()
        success = loader.load_all()
        loader.disconnect()
        return success

    # ── Utility ───────────────────────────────────────────────────────────────

    @staticmethod
    def check_java() -> bool:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        if result.returncode == 0:
            logger.debug(f"Java found: {result.stderr.split(chr(10))[0]}")
            return True
        logger.warning("Java not found — neo4j-etl JAR mode requires Java 11+")
        return False

    @staticmethod
    def download_instructions():
        return f"""
neo4j-etl-tool not found. To use JAR mode:

1. Download from: {ETLRunner.DOWNLOAD_URL}
2. Place the JAR at: ./neo4j-etl/neo4j-etl.jar
3. Or set jar_path in config.yaml

Alternatively, enable Docker mode:
  neo4j_etl:
    use_docker: true
    
Or simply run without it — the pipeline will use the built-in
Python direct loader (Neo4jDirectLoader) as a full replacement.
"""
