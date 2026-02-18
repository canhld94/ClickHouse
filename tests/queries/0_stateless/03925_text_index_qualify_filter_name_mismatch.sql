-- Regression test for a bug where text index preprocessing modified the filter DAG
-- (recreating the AND function node with a different result_name) but
-- processAndOptimizeTextIndexDAG returned nullptr because no virtual columns were added,
-- causing the FilterStep's filter_column_name to become inconsistent with the DAG.
-- This triggered a LOGICAL_ERROR in applyOrder.

DROP TABLE IF EXISTS t_text_index_qualify;

CREATE TABLE t_text_index_qualify
(
    id UInt64,
    val Map(String, String),
    INDEX idx mapValues(val) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_text_index_qualify VALUES (1, {'a': 'foo'}), (2, {'b': 'bar'});

-- The combination of PREWHERE + WHERE + QUALIFY with a text-indexed function triggers the bug.
-- QUALIFY merges into WHERE as AND, then text index preprocessing rewrites hasAnyTokens
-- but returns nullptr (no virtual columns added), leaving filter_column_name stale.
SELECT DISTINCT id
FROM t_text_index_qualify
PREWHERE hasAnyTokens(mapValues(val), 'foo')
WHERE hasAnyTokens(mapValues(val), 'foo')
QUALIFY id
ORDER BY id;

DROP TABLE t_text_index_qualify;
