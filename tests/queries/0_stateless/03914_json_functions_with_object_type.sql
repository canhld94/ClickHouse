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

-- Cleanup
DROP TABLE json_test;

SELECT 'All tests completed';
