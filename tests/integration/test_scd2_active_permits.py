from dataclasses import replace
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from toronto_housing_ingestion.config import load_config
from toronto_housing_ingestion.dlt_loader import load_scd2


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "data" / "raw" / "active_permits"

snapshots = list(SOURCE_DIR.glob("*/*/data.parquet"))

if not snapshots:
    raise FileNotFoundError(
        f"No Active Permits snapshot found in {SOURCE_DIR}"
    )

SOURCE_FILE = max(snapshots, key=lambda path: path.parent.name)


def test_active_permits_scd2(tmp_path):
    print("\n[1/6] Reading original snapshot...")
    original = pd.read_parquet(SOURCE_FILE)
    print(f"    rows: {len(original):,}")

    print("[2/6] Creating changed snapshot...")

    updated = original.copy()

    # Change one existing permit.
    changed_id = updated.loc[updated.index[0], "_id"]
    updated.loc[updated.index[0], "permit_type"] = "SCD2_TEST_CHANGED"

    # Delete one permit.
    deleted_id = updated.loc[updated.index[1], "_id"]
    updated = updated.drop(updated.index[1])

    # Insert one new permit.
    new_row = updated.iloc[0].copy()

    if pd.api.types.is_integer_dtype(updated["_id"]):
        new_id = int(updated["_id"].max()) + 1_000_000
    else:
        new_id = "SCD2_TEST_NEW"

    new_row["_id"] = new_id

    updated = pd.concat(
        [updated, pd.DataFrame([new_row])],
        ignore_index=True,
    )

    updated_file = tmp_path / "updated.parquet"
    database = tmp_path / "test.duckdb"

    pq.write_table(
        pa.Table.from_pandas(updated, preserve_index=False),
        updated_file,
    )

    print(f"    original rows: {len(original):,}")
    print(f"    updated rows:  {len(updated):,}")
    print("    changed: 1 permit")
    print("    deleted: 1 permit")
    print("    inserted: 1 permit")

    config = load_config(PROJECT_ROOT / "open_data_config.yml")

    source = next(
        source
        for source in config.sources
        if source.name == "active_permits"
    )

    # Temporary key used only to test SCD2 mechanics.
    source = replace(
        source,
        warehouse_load=True,
        primary_key=("_id",),
        warehouse_since=None,
        warehouse_date_column=None,
    )

    print("[3/6] Loading original snapshot...")
    load_scd2(
        credentials=str(database),
        snapshot_path=SOURCE_FILE,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )
    print("    original snapshot loaded")

    print("[4/6] Loading changed snapshot...")
    load_scd2(
        credentials=str(database),
        snapshot_path=updated_file,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )
    print("    changed snapshot loaded")

    print("[5/6] Reloading unchanged snapshot...")
    load_scd2(
        credentials=str(database),
        snapshot_path=updated_file,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )
    print("    unchanged snapshot reloaded")

    print("[6/6] Validating SCD2 history...")

    with duckdb.connect(str(database), read_only=True) as connection:
        total = connection.execute(
            "SELECT COUNT(*) FROM raw.raw_active_permits"
        ).fetchone()[0]

        current = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.raw_active_permits
            WHERE _dlt_valid_to IS NULL
            """
        ).fetchone()[0]

        changed_versions = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.raw_active_permits
            WHERE _id = ?
            """,
            [changed_id],
        ).fetchone()[0]

        deleted_current = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.raw_active_permits
            WHERE _id = ?
              AND _dlt_valid_to IS NULL
            """,
            [deleted_id],
        ).fetchone()[0]

        new_versions = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.raw_active_permits
            WHERE _id = ?
            """,
            [new_id],
        ).fetchone()[0]

    print(f"    total rows: {total:,}")
    print(f"    current rows: {current:,}")
    print(f"    changed permit versions: {changed_versions}")
    print(f"    deleted permit current versions: {deleted_current}")
    print(f"    inserted permit versions: {new_versions}")

    original_rows = len(original)

    assert total == original_rows + 1
    assert current == original_rows
    assert changed_versions == 2
    assert deleted_current == 0
    assert new_versions == 1

    print("    SCD2 validation passed")

"""
Run it with:

```bash
pytest -s -q tests/integration/test_scd2_active_permits.py
```

The `-s` is important because it allows the progress messages and dlt's progress output to appear in the terminal.
"""