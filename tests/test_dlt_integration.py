from pathlib import Path

import pytest

pytest.importorskip("dlt")
pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

import pandas as pd

from toronto_housing_ingestion.config import SourceConfig
from toronto_housing_ingestion.dlt_loader import load_scd2


def test_scd2_same_snapshot_is_idempotent(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    snapshot = tmp_path / "snapshot.parquet"
    table = pa.Table.from_pandas(
        pd.DataFrame({"id": [1, 2], "status": ["open", "open"]}),
        preserve_index=False,
    )
    pq.write_table(table, snapshot)

    source = SourceConfig(
        name="test",
        title="Test",
        dataset_slug="test",
        role="test",
        expected_cadence="daily",
        portal_url="https://example.com",
        required_columns=("id",),
        date_columns=(),
        pagination_sort="id asc",
        warehouse_table="test",
        warehouse_load=True,
        primary_key=("id",),
        type_hints={},
    )
    db = tmp_path / "test.duckdb"
    result1 = load_scd2(
        credentials=str(db),
        snapshot_path=snapshot,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )
    result2 = load_scd2(
        credentials=str(db),
        snapshot_path=snapshot,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )
    assert result1.current_rows == 2
    assert result2.total_rows == 2
    assert result2.current_rows == 2
