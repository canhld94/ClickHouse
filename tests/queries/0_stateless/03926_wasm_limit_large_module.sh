#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./wasm_udf.lib
. "$CUR_DIR"/wasm_udf.lib


${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS access_data;
DELETE FROM system.webassembly_modules WHERE name = 'large_module';
EOF

load_wasm_module large_module

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE OR REPLACE FUNCTION access_data LANGUAGE WASM ABI PLAIN FROM 'large_module' ARGUMENTS (UInt32) RETURNS Int32 SETTINGS max_memory = 655360;
SELECT access_data(0 :: UInt32) == 42; -- { serverError WASM_ERROR }

CREATE OR REPLACE FUNCTION access_data LANGUAGE WASM ABI PLAIN FROM 'large_module' ARGUMENTS (UInt32) RETURNS Int32 SETTINGS max_memory = 6553600;
SELECT access_data(0 :: UInt32);

EOF
