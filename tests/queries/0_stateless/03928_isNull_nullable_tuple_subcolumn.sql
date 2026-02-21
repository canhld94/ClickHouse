-- Tags: no-ordinary-database

-- Regression test for LOGICAL_ERROR: Bad cast from ColumnNullable to ColumnVector<unsigned long>
-- The FunctionToSubcolumnsPass replaced isNull(col) with reading the .null subcolumn,
-- hardcoding its type as UInt8. But for Nullable(Tuple(... Nullable(T) ...)),
-- the .null subcolumn in storage is Nullable(UInt8), causing a type mismatch.

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_nullable_tuple;

CREATE TABLE t_nullable_tuple
(
    `tup` Nullable(Tuple(u Nullable(UInt64), s Nullable(String)))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0, nullable_serialization_version = 'allow_sparse', min_bytes_for_wide_part = 0;

INSERT INTO t_nullable_tuple SELECT if((number % 5) = 0, (number, toString(number)), NULL) FROM numbers(1000);

-- These used to cause LOGICAL_ERROR in debug/sanitizer builds.
SELECT sum(toUInt64(isNull(tup.s))) AS null_s, sum(toUInt64(isNull(tup.u))) AS null_u FROM t_nullable_tuple FORMAT Null;
SELECT sum(toUInt64(isNotNull(tup.s))) AS notnull_s, sum(toUInt64(isNotNull(tup.u))) AS notnull_u FROM t_nullable_tuple FORMAT Null;

-- Also test without inner Nullable — this exercises the normal optimization path
-- and should produce correct results (800 out of 1000 tuples are NULL).
DROP TABLE t_nullable_tuple;

CREATE TABLE t_nullable_tuple
(
    `tup` Nullable(Tuple(u UInt64, s String))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0, nullable_serialization_version = 'allow_sparse', min_bytes_for_wide_part = 0;

INSERT INTO t_nullable_tuple SELECT if((number % 5) = 0, (number, toString(number)), NULL) FROM numbers(1000);

SELECT sum(toUInt64(isNull(tup.s))) AS null_s, sum(toUInt64(isNull(tup.u))) AS null_u FROM t_nullable_tuple;
SELECT sum(toUInt64(isNotNull(tup.s))) AS notnull_s, sum(toUInt64(isNotNull(tup.u))) AS notnull_u FROM t_nullable_tuple;
SELECT count() FROM t_nullable_tuple WHERE isNull(tup.s);
SELECT count() FROM t_nullable_tuple WHERE isNotNull(tup.s);

SELECT 'OK';

DROP TABLE t_nullable_tuple;
