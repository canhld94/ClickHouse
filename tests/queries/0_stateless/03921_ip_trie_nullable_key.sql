-- https://github.com/ClickHouse/ClickHouse/issues/97454
-- ip_trie key must be String; any other type could cause a logical error in the read path.

DROP DICTIONARY IF EXISTS 03921_d0;
DROP VIEW IF EXISTS 03921_v0;

CREATE VIEW 03921_v0 AS (SELECT 1 c0);

-- CREATE would succeed (loading is async) but SELECT should load the dictionary and must throw BAD_ARGUMENTS in this case.
CREATE DICTIONARY 03921_d0 (c0 Nullable(String))
PRIMARY KEY (c0)
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE '03921_v0'))
LAYOUT(IP_TRIE())
LIFETIME(1);

SELECT c0.null FROM 03921_d0;  -- {serverError BAD_ARGUMENTS}

DROP DICTIONARY IF EXISTS 03921_d0;
DROP VIEW IF EXISTS 03921_v0;
