package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

func seedNMSdb() error {
	pgDSNCount := os.Getenv("PG_DSN_COUNT")
	dsnCount, err := strconv.ParseInt(pgDSNCount, 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing pg_dsn_count")
	}
	if dsnCount < 1 {
		return fmt.Errorf("missing or invalid env var: pg_dsn_count")
	}
	nmsDB, err := nmsDBOpen()
	if err != nil {
		return fmt.Errorf("seed nmsdbopen error: %v", err)
	}
	defer nmsDB.Close()
	for i := 1; i <= int(dsnCount); i++ {
		var dsnEnum int64 = int64(i)
		var nmsColumn string
		var dbSchema string
		dbURL := os.Getenv("PG_DB_URL_" + strconv.FormatInt(dsnEnum, 10))
		nmsColumn = os.Getenv("PG_NMS_COLUMN_" + strconv.FormatInt(dsnEnum, 10))
		dbSchema = os.Getenv("PG_SCHEMA_NAME_" + strconv.FormatInt(dsnEnum, 10))
		log.Printf("seed loop: %v\tdsnEnum %v\tdbURL %v\tnmsColumn %v\n", i, dsnEnum, dbURL, nmsColumn)
		if dbURL == "" || nmsColumn == "" || dbSchema == "" {
			return fmt.Errorf("seednmsdb() error: missing environment variables")
		}
		pgPool, err := getPGConnection(dbURL)
		if err != nil {
			return fmt.Errorf("pg connection error: %v", err)
		} else {
			log.Println("PG Connected")
		}
		var pgUnlogged []string
		/*
			// unlogged tables are just placeholders in postgres replicas, remove them from table list
			if os.Getenv("PG_DB_IS_REPLICA_"+strconv.FormatInt(dsnEnum, 10)) == "true" {
				pgUnlogged, err = getUnloggedTables(pgPool)
				if err != nil {
					return fmt.Errorf("seed getunloggedtables error: %v", err)
				}
			}
		*/
		_, err = getTablesWithNMS(dbSchema, nmsColumn, dsnEnum, pgUnlogged, pgPool, nmsDB)
		if err != nil {
			log.Printf("seed gettableswithnms error: %v\n", err)
		}
		pgPool.Close()
	}
	_, err = nmsTablesQuery(nmsDB, true)
	if err != nil {
		return fmt.Errorf("seedstatebackup error: %v", err)
	}
	return nil
}
