#!/usr/bin/env python3
# Copyright 2025 Columnar Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pathlib import Path
import time

ACCOUNT = "lgyanbp-im41116"
USER = "ARROW_OSS_USER"
WAREHOUSE = "ARROW_OSS_WH"
ROLE = "ARROW_OSS_ROLE"

private_key_file = str(Path.home() / ".ssh" / "sf_private_key.p8")
rows_to_fetch = 1_000_000

def run_odbc():
    import pyodbc

    connection_string = (
        "DSN=snowflake;"
        f"account={ACCOUNT};"
        f"server={ACCOUNT}.snowflakecomputing.com;"
        f"uid={USER};"
        f"warehouse={WAREHOUSE};"
        f"role={ROLE};"
        "authenticator=SNOWFLAKE_JWT;"
        "PRIV_KEY_FILE=" + private_key_file + ";"
    )

    start_time = time.perf_counter()
    with pyodbc.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS LIMIT {rows_to_fetch}")
            rows = cursor.fetchall()
            print(f"Fetched {len(rows)} rows")
    end_time = time.perf_counter()
    print(f"Time taken: {end_time - start_time:.2f} seconds")

def run_adbc():
    import adbc_driver_manager.dbapi as adbc

    connection_opts = {
        "username": USER,
        "adbc.snowflake.sql.auth_type": "auth_jwt",
        "adbc.snowflake.sql.client_option.jwt_private_key": private_key_file,
        "adbc.snowflake.sql.warehouse": WAREHOUSE,
        "adbc.snowflake.sql.role": ROLE,
        "adbc.snowflake.sql.account": ACCOUNT,
    }

    start_time = time.perf_counter()
    with adbc.connect(driver="snowflake", db_kwargs=connection_opts) as conn:
        with conn.cursor() as cursor:            
            cursor.execute(f"SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS LIMIT {rows_to_fetch}")
            rows = cursor.fetch_polars()
            print(f"Fetched {len(rows)} rows")
    end_time = time.perf_counter()
    print(f"Time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    run_odbc()
    run_adbc()
