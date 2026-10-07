# Bronze pipeline implementation summary

This is a reconstruction from the repository's
[`bronze-ingestion-design.md`](./bronze-ingestion-design.md), not a recovered
copy of the chat-only plan. The original conversation was not present in the
session history available here, so exact wording or details from that plan
cannot be verified. This document is a useful reference for the module
boundaries, function hierarchy, and bronze validation design currently recorded
in the repository; some functions remain scaffolds and policy choices are not
yet settled.

## Pipeline hierarchy

The application entry point owns Spark creation and shutdown. The orchestration
layer receives that Spark session and coordinates the work; it should not
contain the CSV, validation, or Delta implementation details.

```text
application entry point
└── create SparkSession and BronzeIngestionConfig
    └── ingest_batch(spark, config, entities)
        ├── create one pipeline run ID for the batch
        └── ingest_entity(spark, config, entity, pipeline_run_id)
            ├── load_entity_data(...) → LoadedEntityData
            │   ├── resolve_source_path(...)
            │   ├── resolve_entity_schema(...)
            │   └── read_source_csv(...)
            ├── add_metadata_columns(...) → DataFrame
            │   ├── add_record_hash(...)
            │   └── add_ingestion_metadata(...)
            ├── validate_bronze_data(df, entity)
            │   ├── check_required_columns(df, entity)
            │   ├── collect_validation_metrics(df)
            │   ├── build ValidationReport
            │   └── apply_validation_policy(report)
            ├── write_bronze_data(...) → WriteMetrics
                ├── prepare_merge_source(...)
                ├── create_delta_table(...)  [first write]
                └── merge_new_records(...)   [subsequent writes]
            └── append_audit_record(...) → bronze/ingestion_audit
```

`ingest_batch` is the batch-level orchestrator: create one run ID and reuse it
for every entity. `ingest_entity` is the entity-level orchestrator: call the
focused operations in order—load, enrich, validate, write. The focused modules
should not import the orchestration module.

## Module/function responsibilities

| Module | Responsibility | Main functions/types |
| --- | --- | --- |
| `src/bronze_ingestion.py` | Coordinate batch and entity workflow | `BronzeIngestionConfig`, `ingest_batch`, `ingest_entity` |
| `src/bronze_source.py` | Resolve and read source CSV data | `resolve_source_path`, `resolve_entity_schema`, `read_source_csv`, `load_entity_data` |
| `src/bronze_metadata.py` | Add payload hash and ingestion metadata | `add_record_hash`, `add_ingestion_metadata`, `add_metadata_columns` |
| `src/bronze_validation.py` | Check data contract, report quality metrics, enforce policy | `ValidationMetrics`, `ValidationReport`, `check_required_columns`, `collect_validation_metrics`, `apply_validation_policy`, `validate_bronze_data` |
| `src/bronze_writer.py` | Persist validated rows to Delta | `prepare_merge_source`, `create_delta_table`, `merge_new_records`, `write_bronze_data` |
| `src/spark_setup.py` | Create configured Spark session | `get_spark_session` |
| `Configs/bronze_schemas.py` | Store entity source schemas | `SCHEMAS` |

## Bronze validation design

Validation belongs in `src/bronze_validation.py`. It runs **after** metadata
enrichment and **before** the writer. It should inspect and report data quality;
it should not silently repair or deduplicate records.

### Validation flow

1. **Check shape first:** `check_required_columns(df, entity)` returns the
   required column names missing from the DataFrame. Do this before row-level
   checks that depend on those columns.
2. **Collect metrics:** `collect_validation_metrics(df)` computes the quality
   counts, preferably together in one Spark aggregation.
3. **Build a report:** `ValidationReport` associates the entity, metrics, and
   validation errors. Its `is_valid` property represents whether there are
   blocking errors.
4. **Apply policy:** the caller passes the report to
   `apply_validation_policy(report)`. Corrupt records produce a warning;
   blocking errors fail the entity.
5. **Coordinate:** `validate_bronze_data(df, entity)` runs the checks and
   returns the report for the caller to inspect and apply.

### Planned metrics

The current `ValidationMetrics` model lists:

- `row_count`
- `null_or_blank_source_record_id_count`
- `duplicate_source_record_id_count`
- `corrupt_record_count`
- `null_record_hash_count`

The checks should produce observable counts and errors. A nonzero corrupt
record count is a warning; nonzero counts for the other quality metrics are
blocking errors under the current policy.

### Tests to build around validation

- Required columns present and missing (including an entity-specific error).
- Null and blank `source_record_id` values counted correctly.
- Duplicate source IDs counted correctly.
- Corrupt-record rows counted correctly.
- Null `_record_hash` values counted correctly.
- Metrics/report identify the entity and have expected values.
- `is_valid` correctly reflects blocking errors.
- Policy behavior for a valid report and each blocking-error case.
- `validate_bronze_data` composes the checks and returns its report.

### Decisions still needed

- Which required columns apply to each entity, and how that contract is
  represented.
- Whether duplicate identity uses `source_record_id`, payload hash, or a
  composite key such as `(source_system, source_record_id)`.
- Whether invalid data blocks the entity, is quarantined, or is retained with
  quality metadata.
- Whether duplicates within one batch are retained, audited, or reduced.

Do not make duplicate handling implicit in `collect_validation_metrics` or
silently drop rows in validation. Keep detection/reporting separate from the
chosen action.

## Implementation sequence

Implement from leaf operations upward:

1. Source path/schema resolution and CSV reading.
2. Metadata and hash behavior; confirm that the hash excludes replay-varying
   ingestion metadata.
3. Individual validation checks and report/policy behavior.
4. Delta first-write and merge operations.
5. Module-level coordinators: `load_entity_data`,
   `validate_bronze_data`, and `write_bronze_data`.
6. Top-level orchestration: `ingest_entity`, then `ingest_batch`.

Use a Spark fixture and temporary paths in tests. Spark session lifecycle stays
in the test/entry-point layer, not in ingestion helpers.

All source-batch configuration and source-loading APIs use
`source_batch_id` consistently.

Each entity ingestion returns an `IngestionAuditRecord` and appends it to
`bronze_root / "ingestion_audit"`. The record combines source provenance,
validation metrics, Delta write metrics, status, and any failure message.
