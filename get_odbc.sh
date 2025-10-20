#!/usr/bin/env bash

set -euo pipefail

TGZ_FILE="snowflake_linux_x8664_odbc-3.12.0.tgz"
DOWNLOAD_URL="https://sfc-repo.snowflakecomputing.com/odbc/linux/3.12.0/${TGZ_FILE}"

curl -fsSL "${DOWNLOAD_URL}" -o "${TGZ_FILE}"
tar -xzvf "${TGZ_FILE}"
sed -i 's/SnowflakeDSII/snowflake/' snowflake_odbc/conf/odbc.ini
sed -i "s|^Driver=.*$|Driver=$PIXI_PROJECT_ROOT/snowflake_odbc/lib/libSnowflake.so|" snowflake_odbc/conf/odbc.ini
sed -i "s|^Driver=.*$|Driver=$PIXI_PROJECT_ROOT/snowflake_odbc/lib/libSnowflake.so|" snowflake_odbc/conf/odbcinst.ini
cp -r ./snowflake_odbc/ErrorMessages/en-US ./snowflake_odbc/lib
