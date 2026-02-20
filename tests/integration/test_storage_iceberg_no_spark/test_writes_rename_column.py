import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_writes_rename_column(started_cluster_iceberg_no_spark, format_version, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_writes_rename_column_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        format_version,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'hello'), (2, 'world');",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id") == "1\thello\n2\tworld\n"

    instance.query(
        f"ALTER TABLE {TABLE_NAME} RENAME COLUMN value TO label;",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert instance.query(f"SELECT id, label FROM {TABLE_NAME} ORDER BY id") == "1\thello\n2\tworld\n"

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (3, 'foo');",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert instance.query(f"SELECT id, label FROM {TABLE_NAME} ORDER BY id") == "1\thello\n2\tworld\n3\tfoo\n"
