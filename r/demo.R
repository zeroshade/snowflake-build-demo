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

# run this first to install the package
# install.packages("adbcdrivermanager")

private_key_file <- path.expand(file.path("~", ".ssh", "sf_private_key.p8"))

ACCOUNT <- "lgyanbp-im41116"
USER <- "ARROW_OSS_USER"
WAREHOUSE <- "ARROW_OSS_WH"
ROLE <- "ARROW_OSS_ROLE"

library(odbc)

con <- dbConnect(odbc(),
    DSN = "snowflake",
    account = ACCOUNT,
    uid = USER,
    warehouse = WAREHOUSE,
    role = ROLE,
    authenticator = "SNOWFLAKE_JWT",
    priv_key_file = private_key_file)

print("With ODBC:")
system.time({
    result <- con |>
        dbGetQuery("SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS LIMIT 1000000") |>
        tibble::as_tibble()
})

library(adbcdrivermanager)

drv <- adbc_driver("snowflake")

db <- adbc_database_init(
    drv,
    username=USER,
    adbc.snowflake.sql.auth_type="auth_jwt",
    adbc.snowflake.sql.client_option.jwt_private_key=private_key_file,
    adbc.snowflake.sql.warehouse=WAREHOUSE,
    adbc.snowflake.sql.role=ROLE,
    adbc.snowflake.sql.account=ACCOUNT
)

con <- adbc_connection_init(db)
print("With ADBC:")
system.time({
    result <- con |>
        read_adbc("SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS LIMIT 1000000") |>
        tibble::as_tibble()
})
