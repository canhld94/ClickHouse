#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./wasm_udf.lib
. "$CUR_DIR"/wasm_udf.lib

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS test_host_api;
DROP FUNCTION IF EXISTS test_func;
DROP FUNCTION IF EXISTS export_faulty_malloc;
DROP FUNCTION IF EXISTS export_incorrect_malloc;

DELETE FROM system.webassembly_modules WHERE name = 'test_host_api';
DELETE FROM system.webassembly_modules WHERE name = 'import_unknown';
DELETE FROM system.webassembly_modules WHERE name = 'import_incorrect';
DELETE FROM system.webassembly_modules WHERE name = 'export_incorrect_malloc';
DELETE FROM system.webassembly_modules WHERE name = 'export_faulty_malloc';

EOF

load_wasm_module host_api test_host_api

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION test_host_api LANGUAGE WASM ABI PLAIN FROM 'test_host_api' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32;

SELECT test_host_api(materialize(0) :: UInt32) SETTINGS log_comment = '03208_wasm_import_export_ok' FORMAT Null;
SELECT test_host_api(materialize(1) :: UInt32) SETTINGS log_comment = '03208_wasm_import_export_err' FORMAT Null; -- { serverError WASM_ERROR }

DROP FUNCTION IF EXISTS test_host_api;

SYSTEM FLUSH LOGS text_log;
SYSTEM FLUSH LOGS query_log;

SELECT count() >= 1
FROM system.text_log
WHERE event_date >= yesterday() AND query_id = (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND query LIKE '%test_host_api%'
    AND type = 'QueryFinish' AND query_kind = 'Select'
    AND log_comment = '03208_wasm_import_export_ok'
    AND current_database = currentDatabase()
    ORDER BY event_time DESC
    LIMIT 1
)
AND message LIKE 'Hello, ClickHouse%' || (SELECT value FROM system.build_options WHERE name = 'VERSION_INTEGER') || '%!'
;

SELECT exception LIKE '%Goodbye, ClickHouse%'
FROM system.query_log
WHERE event_date >= yesterday() AND query LIKE '%test_host_api%'
AND query_kind = 'Select' AND toString(type) LIKE 'Exception%'
AND log_comment = '03208_wasm_import_export_err'
AND current_database = currentDatabase()
ORDER BY event_time DESC
LIMIT 1
;

EOF

load_wasm_module import_unknown

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION test_func LANGUAGE WASM ABI PLAIN FROM 'import_unknown' ARGUMENTS (UInt32) RETURNS UInt32; -- { serverError RESOURCE_NOT_FOUND }
DELETE FROM system.webassembly_modules WHERE name = 'import_unknown';

EOF

load_wasm_module import_incorrect

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION test_func LANGUAGE WASM ABI PLAIN FROM 'import_incorrect' ARGUMENTS (UInt32) RETURNS UInt32;  -- { serverError BAD_ARGUMENTS }
DELETE FROM system.webassembly_modules WHERE name = 'import_incorrect';

EOF

load_wasm_module export_incorrect_malloc

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION export_incorrect_malloc LANGUAGE WASM ABI UNSTABLE_V0_1 FROM 'export_incorrect_malloc' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32; -- { serverError BAD_ARGUMENTS }
DELETE FROM system.webassembly_modules WHERE name = 'export_incorrect_malloc';

EOF

load_wasm_module export_faulty_malloc

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION export_faulty_malloc LANGUAGE WASM ABI UNSTABLE_V0_1 FROM 'export_faulty_malloc' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT export_faulty_malloc(1 :: UInt32); -- { serverError WASM_ERROR }

EOF
