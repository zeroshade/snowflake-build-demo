// Copyright 2025 Columnar Technologies Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/alexbrainman/odbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
)

var (
	privateKeyFile = os.ExpandEnv("$HOME/.ssh/sf_private_key.p8")
	rowsToFetch    = 1000000
)

const (
	ACCOUNT   = "lgyanbp-im41116"
	USER      = "ARROW_OSS_USER"
	WAREHOUSE = "ARROW_OSS_WH"
	ROLE      = "ARROW_OSS_ROLE"
)

func RunADBC() {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":                       "snowflake",
		"username":                     USER,
		"adbc.snowflake.sql.auth_type": "auth_jwt",

		"adbc.snowflake.sql.client_option.jwt_private_key": privateKeyFile,

		"adbc.snowflake.sql.warehouse": WAREHOUSE,
		"adbc.snowflake.sql.role":      ROLE,
		"adbc.snowflake.sql.account":   ACCOUNT,
	})

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	query := fmt.Sprintf("SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS LIMIT %d", rowsToFetch)
	if err := stmt.SetSqlQuery(query); err != nil {
		log.Fatal(err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Release()

	var (
		nrows int64
		// keep the records around so we can do something with them!
		records []arrow.RecordBatch
	)
	for reader.Next() {
		rec := reader.RecordBatch()
		rec.Retain()
		defer rec.Release()
		records = append(records, rec)
		nrows += rec.NumRows()
	}

	if err := reader.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Fetched %d rows, %d records\n", nrows, len(records))
}

func RunODBC() {
	connStr := "DSN=snowflake;" +
		"account=" + ACCOUNT + ";" +
		"uid=" + USER + ";" +
		"warehouse=" + WAREHOUSE + ";" +
		"role=" + ROLE + ";" +
		"authenticator=SNOWFLAKE_JWT;" +
		"priv_key_file=" + privateKeyFile + ";"

	db, err := sql.Open("odbc", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	query := fmt.Sprintf("SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS LIMIT %d", rowsToFetch)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var (
		results       [][]any
		clerk         string
		comment       string
		custkey       float64
		orderdate     time.Time
		orderkey      float64
		orderpriority string
		orderstatus   string
		shippriority  float64
		totalprice    float64
	)

	for rows.Next() {
		err := rows.Scan(&orderkey, &custkey, &orderstatus, &totalprice, &orderdate, &orderpriority, &clerk, &shippriority, &comment)
		if err != nil {
			log.Fatal(err)
		}

		results = append(results, []any{clerk, comment, custkey, orderdate, orderkey, orderpriority, orderstatus, shippriority, totalprice})
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Fetched %d rows\n", len(results))
}

func main() {
	start := time.Now()
	RunODBC()
	fmt.Printf("ODBC took %s\n", time.Since(start))

	start = time.Now()
	RunADBC()
	fmt.Printf("ADBC took %s\n", time.Since(start))
}
