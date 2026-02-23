DROP TABLE IF EXISTS t_dt64_preepoch;

CREATE TABLE t_dt64_preepoch (
    project_id String,
    id String,
    timestamp DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (project_id, timestamp);

INSERT INTO t_dt64_preepoch VALUES ('p1', 'a', '2026-02-21 18:05:58.394');

SELECT 'DateTime64: without filter';
SELECT project_id, id FROM t_dt64_preepoch WHERE project_id = 'p1' AND id = 'a';

SELECT 'DateTime64: with pre-epoch filter';
SELECT project_id, id FROM t_dt64_preepoch WHERE project_id = 'p1' AND id = 'a' AND timestamp >= '1969-12-31 12:00:00';

SELECT 'DateTime64: with epoch filter';
SELECT project_id, id FROM t_dt64_preepoch WHERE project_id = 'p1' AND id = 'a' AND timestamp >= '1970-01-01 00:00:00';

DROP TABLE t_dt64_preepoch;

DROP TABLE IF EXISTS t_date32_preepoch;

CREATE TABLE t_date32_preepoch (
    id String,
    d Date32
) ENGINE = MergeTree()
PARTITION BY toDate(d)
ORDER BY id;

INSERT INTO t_date32_preepoch VALUES ('a', '2026-02-21');

SELECT 'Date32: without filter';
SELECT id FROM t_date32_preepoch WHERE id = 'a';

SELECT 'Date32: with pre-epoch filter';
SELECT id FROM t_date32_preepoch WHERE id = 'a' AND d >= '1969-12-31';

DROP TABLE t_date32_preepoch;
