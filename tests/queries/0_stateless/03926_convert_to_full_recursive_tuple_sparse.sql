-- Tags: no-random-merge-tree-settings

-- Reproduces assertion failure in Set::appendSetElements when ColumnTuple has
-- inner sparse columns from one MergeTree part and non-sparse from another.
-- The fix: IColumn::convertToFullIfNeeded now recursively converts subcolumns.

SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_sparse_tuple;

CREATE TABLE t_sparse_tuple (key UInt64, val Tuple(a UInt64, b UInt64))
ENGINE = MergeTree ORDER BY key
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

SYSTEM STOP MERGES t_sparse_tuple;

-- Part 1: second tuple element is mostly zeros (defaults) → sparse serialization
INSERT INTO t_sparse_tuple SELECT number, (number, 0) FROM numbers(100);

-- Part 2: second tuple element is non-zero → no sparse serialization
INSERT INTO t_sparse_tuple SELECT number + 200, (number + 200, number + 1) FROM numbers(100);

-- Building a Set from a subquery that reads both parts triggers the bug:
-- first chunk has ColumnTuple with inner ColumnSparse, second chunk has ColumnVector.
SELECT count() FROM t_sparse_tuple WHERE val IN (SELECT val FROM t_sparse_tuple);

DROP TABLE t_sparse_tuple;
