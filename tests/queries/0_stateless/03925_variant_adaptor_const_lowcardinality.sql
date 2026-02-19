-- Regression test: FunctionVariantAdaptor should correctly strip LowCardinality
-- from Const(LowCardinality(...)) results. ColumnConst does not override
-- convertToFullColumnIfLowCardinality, so removeLowCardinalityFromResult must
-- handle the Const wrapper explicitly.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=97242&sha=c49a5e0ed10ad8d8ac924af7287bc1c44116c271&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%29

SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;
SET allow_suspicious_variant_types = 1;

-- Create a Variant column with a LowCardinality variant and compare with NULL.
-- The equals(NULL, x) call produces a Const result inside the variant adaptor,
-- and the LowCardinality wrapper must be stripped from Const(LowCardinality(Nullable(UInt8))).
SELECT NULL = arrayJoin([toLowCardinality(1), toUInt256(2)]);
