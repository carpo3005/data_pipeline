# Bronze ingestion design

## Purpose

The bronze ingestion code is split by responsibility. A small orchestration
layer coordinates the flow, while source loading, metadata enrichment,
validation, and Delta persistence each have their own module.

The current Python files are scaffolds: functions marked
`NotImplementedError` still need implementation. The hierarchy below describes
their intended responsibilities and call order.

## High-level flow

```text
application entry point
└── creates/configures SparkSession and BronzeIngestionConfig
    └── ingest_batch
        └── ingest_entity (once per selected entity)
            ├── load_entity_data
            │   ├── resolve_source_path
            │   ├── resolve_entity_schema
            │   └── read_source_csv
            ├── add_metadata_columns
            │   ├── add_record_hash
            │   └── add_ingestion_metadata
            ├── validate_bronze_data
            │   ├── check_required_columns
            │   ├── collect_validation_metrics
            │   └── apply_validation_policy
            └── write_bronze_data
                ├── prepare_merge_source
                ├── create_delta_table (first write)
                └── merge_new_records (later writes)
```

The application entry point owns Spark session creation and shutdown.
`bronze_ingestion` receives an existing session and does not create or stop it.

## Modules and responsibilities

### `src/bronze_ingestion.py`

The workflow coordinator:

- Holds `BronzeIngestionConfig`, the run-level settings.
- Creates one run ID for a batch.
- Calls `ingest_entity` for each requested entity.
- Defines the order of operations: load, enrich, validate, write.

It should not implement CSV parsing, individual quality rules, or Delta merge
details. Those belong to the modules below.

### `src/bronze_source.py`

Responsible for reading raw source data:

- `resolve_source_path` constructs the input path from the configured root,
  batch ID, and entity.
- `resolve_entity_schema` selects the entity's raw schema and includes the
  corrupt-record column.
- `read_source_csv` applies the CSV read options.
- `load_entity_data` combines path resolution, schema resolution, and reading.

Keep this module independent of ingestion orchestration so source reading can
be tested separately.

### `src/bronze_metadata.py`

Responsible for adding derived and operational metadata:

- `add_record_hash` hashes the selected raw payload columns.
- `add_ingestion_metadata` adds fields such as batch ID, run ID, source system,
  and ingestion timestamp.
- `add_metadata_columns` composes those operations into the enrichment step.

The hash's input columns define payload equality. Exclude values that change
between replays, such as run ID and ingestion timestamp. Keep the selected
columns and serialization rules stable across runs.

### `src/bronze_validation.py`

Responsible for checking the DataFrame against the bronze data contract:

- `check_required_columns` checks its shape before attempting column-based
  checks.
- `collect_validation_metrics` computes row-level counts, preferably with a
  single Spark aggregation where practical.
- `ValidationMetrics` stores those counts.
- `ValidationReport` associates metrics and errors with an entity.
- `validate_bronze_data` coordinates the checks and returns a report.
- `apply_validation_policy` warns for corrupt records and fails the entity
  when the report contains blocking errors.

Corrupt records are measured and reported as warnings, not blocking errors.
Null or blank source IDs, duplicate source IDs, and null record hashes remain
blocking when their metrics are nonzero.

Validation should report or route duplicate data according to an explicit
policy. It should not silently deduplicate rows. Decide separately whether
identity is based on `source_record_id` or payload hash; these represent
different policies.

### `src/bronze_writer.py`

Responsible only for Delta persistence:

- `prepare_merge_source` applies the chosen merge-key behavior to incoming
  rows.
- `create_delta_table` handles a target's first write.
- `merge_new_records` handles subsequent insert-only merges.
- `write_bronze_data` selects the first-write or merge path.

The writer may check technical prerequisites needed to execute a write, such
as whether the merge key is present. Business/data-quality validation belongs
in `bronze_validation.py`, before the writer is called.

### `src/spark_setup.py`

Responsible for building a configured Spark session. Called by an application
entry point or test fixture, not by ingestion helpers.

### `Configs/bronze_schemas.py`

Stores the expected raw source schemas by entity. It contains schema
definitions, not read or validation workflow logic.

## Dependency direction

The intended imports flow toward focused modules:

```text
bronze_ingestion
    ├── bronze_source
    ├── bronze_metadata
    ├── bronze_validation
    └── bronze_writer

bronze_source ──> Configs.bronze_schemas
```

The focused modules should not import `bronze_ingestion`. This avoids circular
dependencies and keeps the orchestration layer at the top of the application
flow.

## Configuration and run state

`BronzeIngestionConfig` contains settings that describe the input/output
locations and source batch: `data_root`, `bronze_root`, `batch_id`, and
`source_system`. Pass these explicitly to the operation that needs them.

The `run_id` is generated once in `ingest_batch` and passed to each entity
ingestion. Avoid module-level mutable run settings; module imports should not
implicitly start or configure a run.

## Suggested implementation and test order

Implement and test the lower-level operations before their orchestrators:

1. Resolve source paths and schemas.
2. Read a small CSV using the explicit schema and configured options.
3. Add the hash and ingestion metadata; verify which columns participate in
   the hash.
4. Implement validation checks and test each contract rule, including invalid
   columns, null/blank IDs, duplicate IDs, corrupt records, and null hashes.
5. Implement the first Delta write, then the merge path; test new rows, replay,
   and duplicates within a single incoming DataFrame.
6. Implement `load_entity_data`, `validate_bronze_data`, and
   `write_bronze_data` as their respective coordinators.
7. Implement `ingest_entity` and `ingest_batch` to wire the tested operations
   together.

Tests should primarily assert observable outcomes. Use a local Spark session
fixture for Spark DataFrame and Delta behavior, and temporary paths for write
tests. Keep the Spark fixture's lifecycle in the test layer; ingestion
functions should not stop a session supplied by their caller.

## Open policy decisions

Before implementing validation and writing, decide and document:

- Is a repeat defined by equal raw payload hash or by equal
  `(source_system, source_record_id)`?
- If the same source ID arrives with a changed payload, should ingestion fail,
  preserve both versions, or update an existing row?
- Should invalid rows fail the entity, be quarantined, or be retained with
  quality metadata?
- Should exact repeated rows within one incoming batch be stored, counted in an
  audit dataset, or reduced to one row in the bronze target?

These choices affect validation and merge behavior; they should remain
explicit instead of being hidden inside generic helpers.
