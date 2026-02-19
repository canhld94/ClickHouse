-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/70449
-- LOGICAL_ERROR "Column identifier is already registered" when reading multiple
-- subcolumns from the same Nested column in a CREATE VIEW / MATERIALIZED VIEW.

CREATE TABLE test_otel_nested
(
    trace_id UUID,
    span_id UInt64,
    parent_span_id UInt64,
    operation_name String,
    start_time_us UInt64,
    finish_time_us UInt64,
    attribute Nested(names String, values String)
) ENGINE = Memory;

INSERT INTO test_otel_nested VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 1, 0, 'test_op', 1000000, 2000000, ['db.statement', 'key2'], ['SELECT 1', 'val2']);

-- Original query from the issue: multiple references to attribute.values and attribute.names
CREATE VIEW test_zipkin AS
SELECT
    lower(hex(reinterpretAsFixedString(trace_id))) AS traceId,
    lower(hex(parent_span_id)) AS parentId,
    lower(hex(span_id)) AS id,
    operation_name AS name,
    start_time_us AS timestamp,
    finish_time_us - start_time_us AS duration,
    cast(tuple('clickhouse'), 'Tuple(serviceName text)') AS localEndpoint,
    cast(tuple(
        attribute.values[indexOf(attribute.names, 'db.statement')]),
        'Tuple("db.statement" text)') AS tags
FROM test_otel_nested;

SELECT tags FROM test_zipkin;

DROP VIEW test_zipkin;

-- Simpler case: two references to subcolumns of the same Nested column
SELECT
    attribute.values[indexOf(attribute.names, 'db.statement')] AS stmt,
    attribute.values[indexOf(attribute.names, 'key2')] AS key2_val
FROM test_otel_nested;

DROP TABLE test_otel_nested;
