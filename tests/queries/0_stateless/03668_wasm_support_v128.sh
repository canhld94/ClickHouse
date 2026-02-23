#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./wasm_udf.lib
. "$CUR_DIR"/wasm_udf.lib

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS concatInts;
DELETE FROM system.webassembly_modules WHERE name = 'test_v128';

EOF

load_wasm_module test_v128

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE OR REPLACE FUNCTION concatInts LANGUAGE WASM ABI PLAIN FROM 'test_v128' ARGUMENTS (UInt64, UInt64) RETURNS UInt128;
SELECT concatInts(1 :: UInt64, 0 :: UInt64);
DROP FUNCTION IF EXISTS concatInts;

EOF
