# Snowflake BUILD Demo

Get Dependencies:

1. Install `dbc` via https://docs.columnar.tech/dbc/getting_started/installation/
2. Install `pixi` if not installed (https://pixi.sh/dev/)
3. Run `pixi install` to install dependencies
4. Run `pixi run get-odbc` to install the snowflake ODBC driver locally if needed
5. Run `pixi run adbc-drivers` to install the ADBC Drivers for snowflake and duckdb

With the dependencies all installed and ready for use, you can run the tests!

## Auth

Update the files in the directories with your Snowflake Auth information. The demo code
currently assumes you have access to [snowflake "key-pair" authentication](https://docs.snowflake.com/en/developer-guide/sql-api/authenticating#label-sql-api-authenticating-key-pair). The file is expected to be in `$HOME/.ssh/sf_private_key.p8` so it must
either be renamed to this or the code should be updated locally to reflect the name of your
auth key.

The files will also need to have the `USER`/`ACCOUNT`/`WAREHOUSE` and `ROLE` constants updated
to match your Snowflake account in order to run the demos.

## Running the demos

### Python

* `pixi run python ./python/timing_fetch.py` -- Run Select query for ODBC and then ADBC with timing
* `pixi run python ./python/ingestion_test.py` -- Ingest from Snowflake into a local ephemeral DuckDB instance

### Go

* `pixi run go run ./go` -- Run ODBC and then ADBC queries with timing

### R

To run the R tests you'll first need to install the ODBC and adbc driver manager packages:

* First run: `pixi run R`
* Then at the prompt run `install.packages("odbc")` and `install.packages("adbcdrivermanager")`

The previous two steps only need to be run once, and then you can run the following to run the demo:

* `pixi run Rscript ./r/demo.R` -- Run ODBC and then ADBC queries with timing

