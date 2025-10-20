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

def ingest_data():
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
        with adbc.connect(driver="duckdb") as duckdb_conn:
            snowflake_cur = conn.cursor()
            duckdb_cur = duckdb_conn.cursor()

            snowflake_cur.execute(f"SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS LIMIT {rows_to_fetch}")
            duckdb_cur.adbc_ingest("orders", snowflake_cur.fetch_record_batch())

            duckdb_cur.execute("SELECT COUNT(*) FROM orders")
            count = duckdb_cur.fetchone()[0]
            print(f"Ingested {count} rows into DuckDB")

            duckdb_cur.execute("SELECT * FROM ORDERS")
            print(duckdb_cur.fetch_polars())
            snowflake_cur.close()
            duckdb_cur.close()

    end_time = time.perf_counter()
    print(f"Time taken for ingestion: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    ingest_data()
