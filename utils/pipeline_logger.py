"""PipelineLogger — buffered logging for bronze ingestion pipelines.

Instantiate early (before config loading). Log entries are buffered in
memory and printed to stdout immediately.  Once the Delta log table
exists, call `set_delta_target()` then `flush()` to persist all
buffered entries.  After that, new entries are written to Delta on
every call (unless `persist_to_delta` is False in the log config).

Usage:
    logger = PipelineLogger(batch_id="...")
    logger.info("PIPELINE", "Pipeline started", details={...})
    ...
    logger.set_delta_target(spark, table_fqn)
    logger.flush()
    ...
    logger.info("INGESTION", "Rows written", entity="bronze_order", details={...})
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any


class PipelineLogger:
    """Buffered pipeline logger with Delta persistence."""

    # Log level ordering for min_log_level filtering
    _LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARN": 2, "ERROR": 3}

    VALID_CATEGORIES = frozenset([
        "PIPELINE", "CONFIG", "SCHEMA", "FILE",
        "INGESTION", "AUDIT", "IDENTIFIER",
    ])

    DEFAULT_TABLE_NAME = "pipeline_log"

    _TABLE_COLUMNS = (
        ("log_id",       "STRING",    False, "UUID per log entry"),
        ("batch_id",     "STRING",    False, "Unique identifier for this pipeline execution batch"),
        ("entity_name",  "STRING",    True,  "Which entity (customer, order, etc.), null for pipeline-level events"),
        ("log_level",    "STRING",    False, "Severity level of the log entry"),
        ("log_category", "STRING",    False, "Functional area that produced the log entry"),
        ("message",      "STRING",    False, "Human-readable log message"),
        ("details",      "STRING",    True,  "JSON blob for structured metadata (row counts, file paths, durations, etc.)"),
        ("logged_at",    "TIMESTAMP", False, "UTC timestamp of when the event was logged"),
    )

    # ── Schema helpers ───────────────────────────────────────────────

    @classmethod
    def schema_ddl(cls) -> str:
        """Return a DDL schema string suitable for createDataFrame."""
        return ", ".join(f"{name} {dtype}" for name, dtype, _, _ in cls._TABLE_COLUMNS)

    @classmethod
    def get_create_table_ddl(cls, table_fqn: str) -> str:
        """Return CREATE TABLE IF NOT EXISTS DDL for the log table."""
        col_defs = []
        for name, dtype, nullable, desc in cls._TABLE_COLUMNS:
            null_str = "" if nullable else " NOT NULL"
            col_defs.append(f"  {name} {dtype}{null_str} COMMENT '{desc}'")
        return (
            f"CREATE TABLE IF NOT EXISTS {table_fqn} (\n"
            + ",\n".join(col_defs)
            + "\n) COMMENT 'Pipeline execution log for bronze ingestion'"
        )

    def __init__(
        self,
        batch_id: str,
        source_system: str | None = None,
    ) -> None:
        self.batch_id = batch_id
        self.source_system = source_system

        # Buffer for entries logged before Delta is ready
        self._buffer: list[dict[str, Any]] = []

        # Delta target (set later via set_delta_target)
        self._spark = None
        self._table_fqn: str | None = None
        self._delta_ready = False

        # Defaults (adjustable via property setters)
        self._min_level = "INFO"
        self._persist = True
        self._print_stdout = True

    # ── Delta target configuration ───────────────────────────────────

    def set_delta_target(self, spark, table_fqn: str) -> None:
        """Configure the Delta log table target."""
        self._spark = spark
        self._table_fqn = table_fqn
        self._delta_ready = True

    # ── Public logging methods ───────────────────────────────────────

    def debug(
        self,
        category: str,
        message: str,
        entity: str | None = None,
        details: dict | None = None,
    ) -> None:
        self._log("DEBUG", category, message, entity, details)

    def info(
        self,
        category: str,
        message: str,
        entity: str | None = None,
        details: dict | None = None,
    ) -> None:
        self._log("INFO", category, message, entity, details)

    def warn(
        self,
        category: str,
        message: str,
        entity: str | None = None,
        details: dict | None = None,
    ) -> None:
        self._log("WARN", category, message, entity, details)

    def error(
        self,
        category: str,
        message: str,
        entity: str | None = None,
        details: dict | None = None,
    ) -> None:
        self._log("ERROR", category, message, entity, details)

    # ── Convenience methods ──────────────────────────────────────────

    def log_pipeline_start(self, details: dict | None = None) -> None:
        """Log a PIPELINE / INFO entry marking the start of a run."""
        self.info("PIPELINE", "Pipeline run started", details=details)

    def log_pipeline_end(
        self,
        status: str = "SUCCESS",
        details: dict | None = None,
    ) -> None:
        """Log a PIPELINE entry marking the end of a run."""
        level = "INFO" if status == "SUCCESS" else "ERROR"
        self._log(
            level, "PIPELINE", f"Pipeline run completed — {status}",
            details=details,
        )

    def log_entity_event(
        self,
        category: str,
        message: str,
        entity: str,
        level: str = "INFO",
        details: dict | None = None,
    ) -> None:
        """Log an event scoped to a specific entity."""
        self._log(level, category, message, entity, details)

    # ── Flush buffer to Delta ────────────────────────────────────────

    def flush(self) -> int:
        """Write all buffered entries to Delta. Returns rows written."""
        if not self._delta_ready or not self._persist:
            return 0

        if not self._buffer:
            return 0

        rows = self._buffer.copy()
        self._buffer.clear()

        df = self._spark.createDataFrame(rows, schema=self.schema_ddl())
        df.write.mode("append").saveAsTable(self._table_fqn)
        return len(rows)

    # ── Internal ─────────────────────────────────────────────────────

    def _log(
        self,
        level: str,
        category: str,
        message: str,
        entity: str | None = None,
        details: dict | None = None,
    ) -> None:
        """Create a log entry, print it, and buffer or persist it."""
        # Filter by minimum log level
        if self._LEVEL_ORDER.get(level, 1) < self._LEVEL_ORDER.get(
            self._min_level, 1
        ):
            return

        entry = {
            "log_id": str(uuid.uuid4()),
            "batch_id": self.batch_id,
            "entity_name": entity,
            "log_level": level,
            "log_category": category,
            "message": message,
            "details": json.dumps(details) if details else None,
            "logged_at": datetime.now(timezone.utc),
        }

        # Print to stdout
        if self._print_stdout:
            ts = entry["logged_at"].strftime("%H:%M:%S")
            entity_tag = f" [{entity}]" if entity else ""
            print(f"[{ts}] {level:<5} | {category:<11}{entity_tag} | {message}")

        # Buffer or write immediately
        if self._delta_ready and self._persist:
            self._buffer.append(entry)
            self.flush()
        else:
            self._buffer.append(entry)

    # ── Utilities ────────────────────────────────────────────────────

    @property
    def buffered_count(self) -> int:
        """Number of entries waiting in the buffer."""
        return len(self._buffer)

    @property
    def is_delta_ready(self) -> bool:
        """Whether the Delta target has been configured."""
        return self._delta_ready

    # ── Settings (read / write) ──────────────────────────────────────

    _KNOWN_SETTINGS = {
        "min_log_level": str,
        "persist_to_delta": bool,
        "print_to_stdout": bool,
    }

    def set_settings(self, **kwargs) -> None:
        """Update one or more logger settings in a single call.

        Accepted keyword arguments:
            min_log_level    (str)  – minimum severity to record (DEBUG/INFO/WARN/ERROR)
            persist_to_delta (bool) – write entries to the Delta log table
            print_to_stdout  (bool) – print entries to notebook stdout

        Raises ValueError for unknown keys or wrong types.
        """
        unknown = set(kwargs) - set(self._KNOWN_SETTINGS)
        if unknown:
            raise ValueError(
                f"Unknown setting(s): {', '.join(sorted(unknown))}. "
                f"Valid settings: {', '.join(sorted(self._KNOWN_SETTINGS))}"
            )

        for key, value in kwargs.items():
            expected_type = self._KNOWN_SETTINGS[key]
            if not isinstance(value, expected_type):
                raise ValueError(
                    f"{key} must be {expected_type.__name__}, got {type(value).__name__}"
                )

        if "min_log_level" in kwargs:
            level = kwargs["min_log_level"].upper()
            if level not in self._LEVEL_ORDER:
                raise ValueError(
                    f"Invalid log level '{level}'. "
                    f"Valid levels: {', '.join(sorted(self._LEVEL_ORDER, key=self._LEVEL_ORDER.get))}"
                )
            self._min_level = level
        if "persist_to_delta" in kwargs:
            self._persist = kwargs["persist_to_delta"]
        if "print_to_stdout" in kwargs:
            self._print_stdout = kwargs["print_to_stdout"]

    @property
    def min_log_level(self) -> str:
        """Current minimum log level."""
        return self._min_level

    @min_log_level.setter
    def min_log_level(self, level: str) -> None:
        self._min_level = level.upper()

    @property
    def persist_to_delta(self) -> bool:
        """Whether entries are written to the Delta log table."""
        return self._persist

    @persist_to_delta.setter
    def persist_to_delta(self, value: bool) -> None:
        self._persist = value

    @property
    def print_to_stdout(self) -> bool:
        """Whether entries are printed to stdout."""
        return self._print_stdout

    @print_to_stdout.setter
    def print_to_stdout(self, value: bool) -> None:
        self._print_stdout = value