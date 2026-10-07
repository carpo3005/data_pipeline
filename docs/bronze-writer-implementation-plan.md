# Bronze writer implementation plan

## Purpose and scope

`src/bronze_writer.py` persists a validated DataFrame as a path-based Delta
table. It creates the table on the first write and performs insert-only merges
on later writes. It does not load source data, validate business quality, or
manage the Spark session lifecycle.

The selected `merge_key` defines identity. With the default `_record_hash`,
rows with the same hash represent the same payload for idempotent writes.
Incoming rows are reduced to one row per key, and only keys not already in the
Delta target are inserted. The writer does not update matched target rows.

This is distinct from source-ID validation. A repeated nonblank
`source_record_id` is a blocking validation error and must stop ingestion
before the writer runs. A repeated `_record_hash` means two rows have the same
payload hash; hash-key deduplication is the writer's idempotency behavior, not
a replacement for source-ID validation. If the business rule requires
preserving separate source records even when their payloads are identical,
`_record_hash` is not a suitable identity key; choose a different merge key.

```text
ingest_entity
  └── validate and apply validation policy
      └── write_bronze_data(spark, df, target, merge_key)
          ├── if target is a Delta table
          │     └── merge_new_records(...): prepare, then insert unmatched keys
          └── otherwise: prepare_merge_source(...)
                └── create_delta_table(...): initial Delta write
```

Validation remains a separate upstream responsibility. The writer should
enforce the technical requirement that the selected merge-key column exists,
but should not duplicate validation rules for null IDs, corrupt records, or
other data-quality conditions.

## Function responsibilities

### `prepare_merge_source(df, merge_key) -> DataFrame`

Prepare incoming rows for an insert-only merge.

- Check that `merge_key` exists in `df`; raise `ValueError` naming it if not.
- Return a DataFrame with the same columns and schema, reduced to at most one
  row per merge-key value.
- Preserve the input DataFrame; Spark transformations return a new DataFrame.
- Do not silently repair missing or invalid key values. Those belong to
  validation.

**Important unresolved detail:** Spark `dropDuplicates([merge_key])` does not
guarantee which row survives when rows share a key but differ in other
columns. With `_record_hash`, the source payloads match but ingestion metadata
may differ. If deterministic metadata retention is required, define a survivor
rule (for example, earliest ingestion timestamp) before implementing it.

### `create_delta_table(spark, df, target) -> None`

Perform the initial write.

- Write `df` using `format("delta")` to `target`.
- Do not overwrite an existing table or arbitrary path. Let the write fail
  explicitly if the target already exists; the dispatcher is responsible for
  selecting the first-write path.
- Preserve the input schema and rows; do not apply merge policy here.
- Do not create or stop `spark`.

### `merge_new_records(spark, df, target, merge_key) -> WriteMetrics`

Insert records whose key is absent from the existing Delta target.

- Open the path-based target with `DeltaTable.forPath`.
- Use a source alias and target alias in a merge condition that compares the
  configured `merge_key`.
- Use `whenNotMatchedInsertAll`; do not update matched records.
- Do not silently create a missing target; `write_bronze_data` selects the
  initial-write path.
- Call `prepare_merge_source` before building the Delta merge so repeated keys
  in the incoming batch cannot cause ambiguous inserts.
- Surface Delta and schema errors rather than masking them.
- Return replay, inserted, hash-conflict, and final target-row counts.
- If a matched composite key has a different `_record_hash`, return a conflict
  count without applying any part of that batch. The ingestion orchestrator
  records the conflict and fails the entity.

### `write_bronze_data(spark, df, target, merge_key) -> WriteMetrics`

Dispatch to initial write or merge.

- Check whether `target` is already a Delta table with
  `DeltaTable.isDeltaTable(spark, str(target))`.
- If the target is a Delta table, insert only unmatched keys.
- Otherwise, prepare the rows and perform the initial Delta write.
- Delegate preparation to the selected branch: the initial-write branch calls
  `prepare_merge_source`; `merge_new_records` prepares its own source. This
  keeps either lower-level operation safe when used directly and avoids
  preparing the source twice.
- Do not overwrite an existing non-Delta path. Fail clearly rather than
  treating arbitrary files as a new table.
- Keep path conversion at the Delta API boundary (`str(target)`); accept
  `Path` in the public interface.

## Testing plan

Use the project Spark fixture and `tmp_path` for isolated path-based Delta
tables. Tests should assert observable table contents and schemas, not internal
Spark plans. Spark session creation and shutdown remain in the test fixture.

### `prepare_merge_source`

- Missing merge-key column raises `ValueError` with the key name.
- Duplicate keys in incoming data produce one row per key.
- Distinct keys are all retained.
- Result columns and schema match the source.
- Document the survivor behavior for same-key rows with differing non-key
  values (or avoid assuming which representative Spark keeps).

### `create_delta_table`

- Writes rows and schema that can be read back with `spark.read.format("delta")`.
- Empty DataFrames with an explicit schema still create a readable Delta table.
- Existing target is not overwritten; assert the operation fails.

### `merge_new_records`

Starting from an existing Delta target:

- Inserts new keys.
- Does not insert keys already present.
- Does not update the values for matched keys.
- Deduplicates repeated keys within one incoming batch.
- Replaying the same input leaves the target unchanged.
- Supports a non-default merge key.
- Does not create a target that does not exist.

### `write_bronze_data`

- First call creates a Delta target.
- A later call merges only new keys.
- Replaying the same batch does not increase target row count and reports
  replay/insert counts.
- `merge_key` is passed through for both first write and later writes.
- A pre-existing non-Delta path fails without being overwritten.

## Decisions and boundaries

- **Agreed:** `merge_key` defines identity; default is `_record_hash`.
- **Agreed:** this is insert-only; matched rows are not updated.
- **Still to decide:** deterministic survivor selection for multiple incoming
  rows sharing a key but having different non-key values.
- **Still to decide:** whether a target schema mismatch should fail or use an
  explicitly enabled Delta schema-evolution policy. Start with fail-fast and
  no implicit schema evolution.
- **Out of scope:** concurrent-writer coordination, partitioning strategy,
  table optimization, deletion, upsert/update semantics, and recovery from
  partial writes.

## Suggested implementation sequence

1. Implement and test `prepare_merge_source`.
2. Implement and test `create_delta_table`.
3. Implement and test `merge_new_records` against an explicitly created target.
4. Implement `write_bronze_data` as the small first-write/merge dispatcher.
5. Run the focused writer tests, then the existing suite.
6. Wire end-to-end ingestion only after validation and writer behavior are
   independently tested.
