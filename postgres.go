package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cast"

	sugar "github.com/waclawthedev/go-sugaring"
)

type pgTable struct {
	nmsTime     time.Time
	dsnEnum     int64
	rowCount    int64
	name        string
	schema      string
	tableSchema string
	nmsColumn   string
	pKeyColumn  string
}

func getPGConnection(dbURL string) (*pgxpool.Pool, error) {
	conf, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse config: %v", err)
	}
	conf.ConnConfig.RuntimeParams["statement_timeout"] = os.Getenv("PG_TIMEOUT_MILLIS")
	conf.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "60000"
	conn, err := pgxpool.NewWithConfig(context.Background(), conf)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %v", err)
	}
	return conn, nil
}

func getTableNMSQuery(tableSchema, tableName, nmsColumn, nms, newNMS string, pgDB *pgxpool.Pool) (string, error) {
	var tableQuery string
	var queryGenTemplate string = `
	SELECT 
		CONCAT(
			'SELECT ', 
				ARRAY_TO_STRING(ARRAY_AGG(CONCAT(column_name, ' ')), ', '),', 
				now () AS snapshot_tm 
			FROM ', table_name, ' 
			WHERE {minfield} > ','''{nms}''
				AND {minfield} <= ''{newtimestamp}''') AS query 
	FROM 
		(SELECT 
			table_name, 
			ordinal_position, 
			CASE 
				WHEN udt_name LIKE '\_%' THEN CONCAT('array_to_json(',column_name,') AS ', column_name) 
				WHEN udt_name LIKE '{percent}vector' THEN CONCAT('array_to_json(',column_name,') AS ', column_name)
				{munging}
				ELSE column_name 
			END AS column_name 
			FROM INFORMATION_SCHEMA.columns 
		WHERE table_schema = '{table_schema}'
		ORDER BY table_name, ordinal_position) AS schema 
	WHERE table_name = '{table_name}' 
	GROUP BY table_name`

	queryGen := strings.Replace(queryGenTemplate, "{percent}", "%", 1)
	var munging string
	if cast.ToBool(os.Getenv("MUNGE_TIMESTAMPS_BEFORE_MIN")) {
		t := cast.ToTime(os.Getenv("MUNGE_MIN_TIMESTAMP"))
		if !t.IsZero() {
			if cast.ToBool(os.Getenv("MUNGE_INVALID_TIMESTAMPS_TO_NULL")) {
				munging = `
				WHEN udt_name IN ('timestamp', 'timestamptz') 
					THEN CONCAT('CASE WHEN ',column_name,' < ''` + t.Format("2006-01-02 15:04:05") + `'' THEN NULL ELSE ', column_name, ' END AS ', column_name)`
			}
			if cast.ToBool(os.Getenv("MUNGE_INVALID_TIMESTAMPS_TO_MIN")) {
				munging = `
					WHEN udt_name IN ('timestamp', 'timestamptz') 
						THEN CONCAT('CASE WHEN ',column_name,' < ''` + t.Format("2006-01-02 15:04:05") + `'' THEN to_timestamp(''` + t.Format("2006-01-02 15:04:05") + `'',''YYYY-MM-DD HH24:MI:SS'') ELSE ', column_name, ' END AS ', column_name)`
			}
		}
	}
	if cast.ToBool(os.Getenv("MUNGE_TIMESTAMPS_BEFORE_EPOCH")) {
		if cast.ToBool(os.Getenv("MUNGE_INVALID_TIMESTAMPS_TO_NULL")) {
			munging = `
			WHEN udt_name IN ('timestamp', 'timestamptz') 
				THEN CONCAT('CASE WHEN ',column_name,' < ''1970-01-01 00:00:00'' THEN NULL ELSE ', column_name, ' END AS ', column_name)`
		}
		if cast.ToBool(os.Getenv("MUNGE_INVALID_TIMESTAMPS_TO_MIN")) {
			t := cast.ToTime(os.Getenv("MUNGE_MIN_TIMESTAMP"))
			if !t.IsZero() && t.Before(time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)) {
				munging = `
				WHEN udt_name IN ('timestamp', 'timestamptz') 
					THEN CONCAT('CASE WHEN ',column_name,' < ''` + t.Format("2006-01-02 15:04:05") + `'' THEN to_timestamp(''` + t.Format("2006-01-02 15:04:05") + `'',''YYYY-MM-DD HH24:MI:SS'') ELSE ', column_name, ' END AS ', column_name)`
			}
		}
	}
	queryGen = strings.Replace(queryGen, "{munging}", munging, 1)
	queryGen = strings.Replace(strings.Replace(strings.Replace(strings.Replace(strings.Replace(queryGen, "{minfield}", nmsColumn, 2), "{newtimestamp}", newNMS, 1), "{table_schema}", tableSchema, 1), "{table_name}", tableName, 1), "{nms}", nms, 1)
	// log.Println(queryGen)
	conn, err := pgDB.Acquire(context.Background())
	if err != nil {
		return "", fmt.Errorf("pg conn acquire error: %v", err)
	}
	defer conn.Release()
	err = conn.QueryRow(context.Background(), queryGen).Scan(&tableQuery)
	if err != nil {
		return "", fmt.Errorf("queryrow failed: %v : %v", tableName, err)
	}
	return tableQuery, nil
}

func getTablePKey(tableName string, pgDB *pgxpool.Pool) (string, error) {
	var pKeyColumn string
	conn, err := pgDB.Acquire(context.Background())
	if err != nil {
		return pKeyColumn, fmt.Errorf("pg conn acquire error: %v", err)
	}
	defer conn.Release()
	pKeyQuery := `SELECT t.table_name, c.column_name, c.ordinal_position
	FROM information_schema.key_column_usage AS c
	LEFT JOIN information_schema.table_constraints AS t
	ON t.constraint_name = c.constraint_name
	WHERE t.table_name = ? AND t.constraint_type = 'PRIMARY KEY';`
	err = conn.QueryRow(context.Background(), pKeyQuery, tableName).Scan(&pKeyColumn)
	if err != nil {
		return pKeyColumn, fmt.Errorf("queryrow failed: %v : %v", tableName, err)
	}
	log.Printf("getTablePKey: %v primary key=%v\n", tableName, pKeyColumn)
	return pKeyColumn, nil
}

func getTableRowCount(tableName string, pgDB *pgxpool.Pool) (int64, error) {
	var rowCount float64
	conn, err := pgDB.Acquire(context.Background())
	if err != nil {
		return 0, fmt.Errorf("pg conn acquire error: %v", err)
	}
	defer conn.Release()
	schemaQuery := `SELECT
	(reltuples/relpages) * (
	  pg_relation_size('` + tableName + `') /
	  (current_setting('block_size')::integer)
	) AS rows
	FROM pg_class where relname = '` + tableName + `';`
	err = conn.QueryRow(context.Background(), schemaQuery).Scan(&rowCount)
	if err != nil {
		return 0, fmt.Errorf("queryrow failed: %v : %v", tableName, err)
	}
	rc := cast.ToInt64(rowCount)
	return rc, nil
}

func getTableSchemaJSON(tableName string, pgDB *pgxpool.Pool) (string, error) {
	var tableSchema string
	// log.Printf("\ngetTableSchemaJSON: %v %v", tableName)
	conn, err := pgDB.Acquire(context.Background())
	if err != nil {
		return "", fmt.Errorf("pg conn acquire error: %v", err)
	}
	defer conn.Release()
	schemaQuery := `select row_to_json(table_schema)
	from (
		select t.table_name, array_agg( c ) as columns
		from information_schema.tables t
		inner join (
			select cl.table_name, cl.column_name, cl.udt_name, cl.is_nullable, cl.ordinal_position,cl.column_default, cl.data_type, cl.character_maximum_length, cl.numeric_precision, cl.numeric_scale, cl.numeric_precision_radix, cl.dtd_identifier, cl.is_identity
			from information_schema.columns cl
		) c (table_name,column_name,udt_name,is_nullable,ordinal_position,column_default,data_type,character_maximum_length,numeric_precision,numeric_scale,numeric_precision_radix,dtd_identifier,is_identity) on c.table_name = t.table_name
		where t.table_type = 'BASE TABLE'
		AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
		AND t.table_name = '` + tableName + `'
		group by t.table_name
	) table_schema;`
	err = conn.QueryRow(context.Background(), schemaQuery).Scan(&tableSchema)
	if err != nil {
		return tableSchema, fmt.Errorf("queryrow failed: %v : %v", tableName, err)
	}
	return tableSchema, nil
}

func getTableSeedNMS(tableName, nmsColumn string, pgDB *pgxpool.Pool) (time.Time, error) {
	var nmsValue time.Time
	conn, err := pgDB.Acquire(context.Background())
	if err != nil {
		return nmsValue, fmt.Errorf("pg conn acquire error: %v", err)
	}
	defer conn.Release()
	err = conn.QueryRow(context.Background(), "SELECT MIN("+nmsColumn+") FROM "+tableName).Scan(&nmsValue)
	if err != nil {
		return nmsValue, fmt.Errorf("queryrow failed: %v : %v", tableName, err)
	}
	log.Printf("getTableSeedNMS: %v.%v=%v\n", tableName, nmsColumn, nmsValue)
	return nmsValue, nil
}

func getTablesWithNMS(tableSchema, nmsColumn string, dsnEnum int64, pgUnlogged []string, pgDB *pgxpool.Pool, nmsDB *sql.DB) ([]pgTable, error) {
	var pgNMSTables []pgTable
	conn, err := pgDB.Acquire(context.Background())
	if err != nil {
		return nil, fmt.Errorf("pg conn acquire error: %v", err)
	}
	defer conn.Release()
	rows, err := conn.Query(context.Background(), "SELECT table_name FROM information_schema.columns WHERE table_schema = '"+tableSchema+"' AND column_name = '"+nmsColumn+"'")
	if err != nil {
		conn.Release()
		return pgNMSTables, fmt.Errorf("query error: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		var table pgTable
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf("gettableswithnms() scan error: %v", err)
		}
		table.name = tableName
		table.schema = tableSchema
		table.nmsColumn = nmsColumn
		table.dsnEnum = dsnEnum
		pgNMSTables = append(pgNMSTables, table)
	}
	// temporary: remove unlogged tables from list of tables to scan
	for i, table := range pgNMSTables {
		var tableName string
		var empty pgTable
		tableName = table.name
		if sugar.Contains(pgUnlogged, tableName) {
			pgNMSTables[i] = pgNMSTables[len(pgNMSTables)-1]
			pgNMSTables[len(pgNMSTables)-1] = empty
			pgNMSTables = pgNMSTables[:len(pgNMSTables)-1]
		}
	}
	for i, table := range pgNMSTables {
		pgNMSTables[i].nmsTime, err = getTableSeedNMS(table.name, table.nmsColumn, pgDB)
		if err != nil {
			continue
		}
		pgNMSTables[i].tableSchema, err = getTableSchemaJSON(table.name, pgDB)
		if err != nil {
			continue
		}
		pgNMSTables[i].rowCount, err = getTableRowCount(table.name, pgDB)
		if err != nil {
			continue
		}
		pgNMSTables[i].pKeyColumn, err = getTablePKey(table.name, pgDB)
		if err != nil {
			continue
		}

		err := seedNMSTable(pgNMSTables[i], nmsDB)
		if err != nil {
			log.Printf("seednmstable() error: %v", err)
			continue
			// return nil, fmt.Errorf("gettableschemajson() error: %v", err)
		}
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("rows error: %v", rows.Err())
	}

	return pgNMSTables, nil
}

func getUnloggedTables(pgDB *pgxpool.Pool) ([]string, error) {
	var pgUnloggedTables []string
	conn, err := pgDB.Acquire(context.Background())
	if err != nil {
		return nil, fmt.Errorf("pg conn acquire error: %v", err)
	}
	rows, err := conn.Query(context.Background(), "select relname from pg_class where relpersistence='u'")
	if err != nil {
		return nil, fmt.Errorf("query error: %v", err)
	}
	defer conn.Release()
	defer rows.Close()
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf("getunloggedtables() scan error: %v", err)
		}
		fmt.Printf("Unlogged: %v\n", tableName)
		pgUnloggedTables = append(pgUnloggedTables, tableName)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("rows error: %v", rows.Err())
	}
	return pgUnloggedTables, nil
}
