-- Tags: no-replicated-database, no-parallel-replicas, no-parallel, no-random-merge-tree-settings
-- EXPLAIN output may differ

SET optimize_use_implicit_projections = 0, session_timezone = 'UTC';

DROP TABLE IF EXISTS t_dt64_explain;

CREATE TABLE t_dt64_explain (id String, ts DateTime64(3))
ENGINE = MergeTree() PARTITION BY toDate(ts) ORDER BY (id, ts);

SYSTEM STOP MERGES t_dt64_explain;

INSERT INTO t_dt64_explain VALUES ('a', '2026-02-20 10:00:00');
INSERT INTO t_dt64_explain VALUES ('b', '2026-02-21 10:00:00');
INSERT INTO t_dt64_explain VALUES ('c', '2026-02-22 10:00:00');

SELECT 'post-epoch: prunes partitions';
EXPLAIN indexes = 1 SELECT id FROM t_dt64_explain WHERE ts >= '2026-02-21 00:00:00';

SELECT 'pre-epoch: no pruning (overflow would give wrong result)';
EXPLAIN indexes = 1 SELECT id FROM t_dt64_explain WHERE ts >= '1969-12-31 12:00:00';

SELECT 'epoch boundary: pruning with correct range';
EXPLAIN indexes = 1 SELECT id FROM t_dt64_explain WHERE ts >= '1970-01-01 00:00:00';

SELECT 'upper bound: no pruning (beyond Date max, overflow wraps)';
EXPLAIN indexes = 1 SELECT id FROM t_dt64_explain WHERE ts >= '2149-06-07 00:00:00';

SELECT 'at Date max: no pruning (boundary value, next wraps)';
EXPLAIN indexes = 1 SELECT id FROM t_dt64_explain WHERE ts >= '2149-06-06 00:00:00';

DROP TABLE t_dt64_explain;
