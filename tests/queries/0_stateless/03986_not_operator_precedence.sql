-- NOT should have lower precedence than IS NULL, IS NOT NULL, IN, BETWEEN, LIKE
-- per the SQL standard. Previously, NOT (expr) IS NULL was parsed as
-- (NOT expr) IS NULL when parentheses wrapped the operand.

-- IS NOT NULL: NOT (1) IS NOT NULL should be NOT (1 IS NOT NULL) = NOT TRUE = 0
SELECT NOT (1) IS NOT NULL;
SELECT NOT 1 IS NOT NULL;

-- IS NULL: NOT (NULL) IS NULL should be NOT (NULL IS NULL) = NOT TRUE = 0
SELECT NOT (NULL) IS NULL;
SELECT NOT NULL IS NULL;

-- IN: NOT (5) IN (1, 2, 5) should be NOT (5 IN (1, 2, 5)) = NOT TRUE = 0
SELECT NOT (5) IN (1, 2, 5);

-- BETWEEN: NOT (5) BETWEEN 1 AND 10 should be NOT (5 BETWEEN 1 AND 10) = NOT TRUE = 0
SELECT NOT (5) BETWEEN 1 AND 10;

-- LIKE: NOT ('abc') LIKE 'a%' should be NOT ('abc' LIKE 'a%') = NOT TRUE = 0
SELECT NOT ('abc') LIKE 'a%';

-- Basic NOT still works
SELECT NOT (0);
SELECT NOT (1);

-- Function call syntax still works
SELECT NOT(1);
SELECT not(0);

-- Complex expression
SELECT NOT (1 + 1) IS NULL;
