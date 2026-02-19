-- Verify that EXCEPT ALL / INTERSECT ALL queries with parentheses
-- format and re-parse correctly (AST formatting roundtrip).
-- This is tested automatically in debug builds via the AST consistency check.

SELECT 1 EXCEPT ALL SELECT 2;
(SELECT 1) EXCEPT ALL (SELECT 2);
(SELECT 1) EXCEPT DISTINCT (SELECT 2);
SELECT number FROM numbers(5) EXCEPT ALL SELECT number FROM numbers(3);
(SELECT number FROM numbers(5)) INTERSECT ALL (SELECT number FROM numbers(3, 5));
