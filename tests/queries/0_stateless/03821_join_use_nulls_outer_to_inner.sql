#!/usr/bin/env -S ${HOME}/clickhouse-client --queries-file

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t2 (id UInt64, val UInt64) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t3 (id UInt64, val2 Nullable(Int64)) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t1 VALUES (4000, 'b'), (5000, 'a');
INSERT INTO t2 VALUES (5000, 1), (5000, 2), (6000, 3);
INSERT INTO t3 VALUES (5000, 1), (5000, 2), (5000, 3), (7000, 10);

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET join_use_nulls = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET optimize_move_to_prewhere = 1;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
FROM t1 AS l
LEFT JOIN t2 AS r ON l.id = r.id
LEFT JOIN t3 AS r2 ON l.id = r2.id
WHERE val > 1
ORDER BY 1;


SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
    FROM t1 AS l
    LEFT JOIN t2 AS r ON l.id = r.id
    LEFT JOIN t3 AS r2 ON l.id = r2.id
    WHERE val > 1
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL') OR trim(explain) LIKE '%Prewhere filter column%';

SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
FROM t1 AS l
FULL JOIN t2 AS r ON l.id = r.id
LEFT JOIN t3 AS r2 ON l.id = r2.id
WHERE val IS NOT NULL
ORDER BY 1;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
    FROM t1 AS l
    FULL JOIN t2 AS r ON l.id = r.id
    LEFT JOIN t3 AS r2 ON l.id = r2.id
    WHERE val IS NOT NULL
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL') OR trim(explain) LIKE '%Prewhere filter column%';

SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
FROM t1 AS l
FULL JOIN t2 AS r ON l.id = r.id
LEFT JOIN t3 AS r2 ON l.id = r2.id
WHERE val IS NOT NULL AND val2 == 3
ORDER BY 1;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
    FROM t1 AS l
    FULL JOIN t2 AS r ON l.id = r.id
    LEFT JOIN t3 AS r2 ON l.id = r2.id
    WHERE val IS NOT NULL AND val2 == 3
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL') OR trim(explain) LIKE '%Prewhere filter column%';


SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
FROM t1 AS l
FULL JOIN t2 AS r ON l.id = r.id
LEFT JOIN t3 AS r2 ON l.id = r2.id
WHERE val IS NULL OR val > 1
ORDER BY 1;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
    FROM t1 AS l
    FULL JOIN t2 AS r ON l.id = r.id
    LEFT JOIN t3 AS r2 ON l.id = r2.id
    WHERE val IS NULL OR val > 1
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL') OR trim(explain) LIKE '%Prewhere filter column%';
