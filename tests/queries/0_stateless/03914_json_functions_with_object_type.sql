SET optimize_functions_to_subcolumns = 1;

CREATE TABLE json_test (
    id Int32,
    json_string String,
    json_object JSON
) ENGINE = Memory;

INSERT INTO json_test VALUES 
    (1, '{"name": "Alice", "age": 30, "scores": [85, 90, 95]}', CAST('{"name": "Alice", "age": 30, "scores": [85, 90, 95]}' AS JSON)),
    (2, '{"name": "Bob", "age": 25, "scores": [75, 80, 88]}', CAST('{"name": "Bob", "age": 25, "scores": [75, 80, 88]}' AS JSON)),
    (3, '{"name": "Charlie", "age": 35, "scores": [92, 88, 90]}', CAST('{"name": "Charlie", "age": 35, "scores": [92, 88, 90]}' AS JSON));

SELECT 'Test 1: JSONExtractString';
SELECT 
    id,
    JSONExtractString(json_string, 'name') as from_string,
    JSONExtractString(json_object, 'name') as from_object
FROM json_test
ORDER BY id;

SELECT 'Test 2: JSONExtractInt';
SELECT 
    id,
    JSONExtractInt(json_string, 'age') as age_from_string,
    JSONExtractInt(json_object, 'age') as age_from_object
FROM json_test
ORDER BY id;

SELECT 'Test 3: JSONExtractUInt';
SELECT 
    id,
    JSONExtractUInt(json_string, 'age') as age_from_string,
    JSONExtractUInt(json_object, 'age') as age_from_object
FROM json_test
ORDER BY id;

SELECT 'Test 4: JSONExtractRaw (array access)';
SELECT 
    id,
    JSONExtractRaw(json_string, 'scores') as scores_from_string,
    JSONExtractRaw(json_object, 'scores') as scores_from_object
FROM json_test
ORDER BY id;

SELECT 'Test 5: JSONExtractInt (nested array)';
SELECT 
    id,
    JSONExtractInt(json_string, 'scores', 1) as first_score_string,
    JSONExtractInt(json_object, 'scores', 1) as first_score_object
FROM json_test
ORDER BY id;

SELECT 'Test 6: JSONHas';
SELECT 
    id,
    JSONHas(json_string, 'name') as has_name_string,
    JSONHas(json_object, 'name') as has_name_object,
    JSONHas(json_string, 'missing_key') as has_missing_string,
    JSONHas(json_object, 'missing_key') as has_missing_object
FROM json_test
ORDER BY id;

SELECT 'Test 7: JSONLength';
SELECT 
    id,
    JSONLength(json_string) as length_string,
    JSONLength(json_object) as length_object,
    JSONLength(json_string, 'scores') as scores_length_string,
    JSONLength(json_object, 'scores') as scores_length_object
FROM json_test
ORDER BY id;

SELECT 'Test 8: JSONType';
SELECT 
    id,
    JSONType(json_string) as type_string,
    JSONType(json_object) as type_object,
    JSONType(json_string, 'name') as name_type_string,
    JSONType(json_object, 'name') as name_type_object,
    JSONType(json_string, 'scores') as scores_type_string,
    JSONType(json_object, 'scores') as scores_type_object
FROM json_test
ORDER BY id;

SELECT 'Test 9: JSONExtract (generic with type)';
SELECT 
    id,
    JSONExtract(json_string, 'age', 'Int32') as age_from_string,
    JSONExtract(json_object, 'age', 'Int32') as age_from_object
FROM json_test
ORDER BY id;

SELECT 'Test 10: isValidJSON';
SELECT 
    id,
    isValidJSON(json_string) as valid_string,
    isValidJSON(json_object) as valid_object
FROM json_test
ORDER BY id;

-- Subcolumn optimization tests 
-- These test the direct path column access optimization
-- (typed/dynamic/shared data paths) vs full row materialization.

SELECT 'Test 11: Nested path (dotted path optimization)';
DROP TABLE IF EXISTS json_nested;
CREATE TABLE json_nested (data JSON) ENGINE = Memory;
INSERT INTO json_nested VALUES ('{"x": {"y": {"z": 42}}}');
INSERT INTO json_nested VALUES ('{"x": {"y": {"z": 99}}}');
SELECT JSONExtractInt(data, 'x', 'y', 'z') as val FROM json_nested ORDER BY val;
DROP TABLE json_nested;

SELECT 'Test 12: Missing path returns default';
SELECT
    JSONExtractInt(json_object, 'nonexistent') as missing_int,
    JSONExtractString(json_object, 'nonexistent') as missing_str
FROM json_test
ORDER BY id;

SELECT 'Test 13: Shared data path (max_dynamic_paths overflow)';
DROP TABLE IF EXISTS json_shared;
CREATE TABLE json_shared (data JSON(max_dynamic_paths=2)) ENGINE = Memory;
INSERT INTO json_shared VALUES ('{"k1": 1, "k2": 2, "k3": 3, "k4": 4, "k5": 5}');
INSERT INTO json_shared VALUES ('{"k1": 10, "k2": 20, "k3": 30, "k4": 40, "k5": 50}');
SELECT JSONExtractInt(data, 'k3') as k3, JSONExtractInt(data, 'k5') as k5 FROM json_shared ORDER BY k3;
DROP TABLE json_shared;

SELECT 'Test 14: ColumnConst JSON object';
SELECT JSONExtractInt(CAST('{"val": 777}' AS JSON), 'val') as const_val;

SELECT 'Test 15: JSONExtractFloat on Object';
DROP TABLE IF EXISTS json_float;
CREATE TABLE json_float (data JSON) ENGINE = Memory;
INSERT INTO json_float VALUES ('{"pi": 3.14159}');
SELECT JSONExtractFloat(data, 'pi') as pi FROM json_float;
DROP TABLE json_float;

SELECT 'Test 16: Fallback path (non-const key)';
SELECT
    JSONExtractInt(json_object, materialize('age')) as age_fallback
FROM json_test
ORDER BY id;

-- Cleanup
DROP TABLE json_test;

-- FunctionToSubcolumnsPass tests 
-- Test that JSONExtract* functions are rewritten to subcolumn reads at the query plan level
-- This is the storage-level I/O optimization: only the specific subcolumn is read from disk

SELECT 'Test 17: FunctionToSubcolumnsPass basic';
DROP TABLE IF EXISTS json_fts_pass;
CREATE TABLE json_fts_pass (data JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO json_fts_pass VALUES ('{"a": 42, "b": "hello", "c": 3.14, "d": true}');
SELECT JSONExtractInt(data, 'a') FROM json_fts_pass SETTINGS optimize_functions_to_subcolumns=1;
SELECT JSONExtractString(data, 'b') FROM json_fts_pass SETTINGS optimize_functions_to_subcolumns=1;
SELECT JSONExtractFloat(data, 'c') FROM json_fts_pass SETTINGS optimize_functions_to_subcolumns=1;
SELECT JSONExtractBool(data, 'd') FROM json_fts_pass SETTINGS optimize_functions_to_subcolumns=1;
SELECT JSONExtractRaw(data, 'a') FROM json_fts_pass SETTINGS optimize_functions_to_subcolumns=1;
SELECT JSONExtract(data, 'a', 'Int64') FROM json_fts_pass SETTINGS optimize_functions_to_subcolumns=1;

SELECT 'Test 18: FunctionToSubcolumnsPass nested path';
DROP TABLE IF EXISTS json_fts_nested;
CREATE TABLE json_fts_nested (data JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO json_fts_nested VALUES ('{"nested": {"x": 100}}');
SELECT JSONExtractInt(data, 'nested', 'x') FROM json_fts_nested SETTINGS optimize_functions_to_subcolumns=1;
DROP TABLE json_fts_nested;

DROP TABLE json_fts_pass;

SELECT 'All tests completed';
