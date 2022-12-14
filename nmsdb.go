package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/benthosdev/benthos/v4/public/service"
	_ "modernc.org/sqlite"
)

type table struct {
	ID           int       `json:"id"`
	NMS          time.Time `json:"nms"`
	NewNMS       time.Time `json:"new_nms"`
	LastShove    time.Time `json:"last_shoved_on"`
	DSNEnum      int64     `json:"dsn_enum"`
	LastRowCount int64     `json:"last_row_count"`
	Name         string    `json:"name"`
	Schema       string    `json:"pg_schema"`
	BQSchema     string    `json:"bq_schema"`
	TableSchema  string    `json:"table_schema"`
	Query        string    `json:"query"`
	stream       *service.Stream
	NMSColumn    string `json:"nms_column"`
}

func nmsDBOpen() (*sql.DB, error) {
	err := os.MkdirAll("sqlite", 0755)
	if err != nil {
		return nil, fmt.Errorf("nmsDBOpen directory create error: %v", err)
	}
	_, e := os.Stat("./sqlite/leftshove-nms.db")
	if !os.IsNotExist(e) {
		db, err := sql.Open("sqlite", "./sqlite/leftshove-nms.db")
		if err != nil {
			return nil, fmt.Errorf("nmsDBOpen file open error: %v", err)
		}
		return db, nil
	} else {
		_, err = os.Create("./sqlite/leftshove-nms.db")
		if err != nil {
			return nil, fmt.Errorf("nmsDBOpen file create error: %v", err)
		} else {
			db, err := sql.Open("sqlite", "./sqlite/leftshove-nms.db")
			if err != nil {
				return nil, fmt.Errorf("nmsDBOpen database connect error: %v", err)
			}
			createStatement := `
			CREATE TABLE nmstables 
			(id INTEGER PRIMARY KEY AUTOINCREMENT, 
			name VARCHAR(64) NOT NULL,
			schema VARCHAR(64) NOT NULL, 
			table_schema VARCHAR, 
			bq_schema VARCHAR, 
			nmsColumn VARCHAR(255) NULL, 
			nms TIMESTAMP NULL,
			last_row_count INTEGER NULL, 
			dsn INTEGER NULL,
			last_shoved_on TIMESTAMP NULL)`
			//"CREATE TABLE `nmstables` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `name` VARCHAR(64) NOT NULL, `schema` VARCHAR(64) NOT NULL, `table_schema` VARCHAR, `bq_schema` VARCHAR, `nmsColumn` VARCHAR(255) NULL, `nms` TIMESTAMP NULL, `dsn` INTEGER NULL)"
			_, err = db.Exec(createStatement)
			if err != nil {
				return nil, fmt.Errorf("nmsDBOpen create table error: %v", err)
			}
			return db, nil
		}
	}
}

func seedNMSTable(pgTableWithNMS pgTable, nmsDB *sql.DB) error {
	var id int64
	err := nmsDB.QueryRow("SELECT id FROM nmstables WHERE name = ? AND dsn = ?", pgTableWithNMS.name, pgTableWithNMS.dsnEnum).Scan(&id)
	if err != nil {
		log.Printf("seedNMSTable() %v: %v\n", pgTableWithNMS.name, err)
	}
	if id > 0 {
		updateQuery := `
		UPDATE nmstables
		SET 
			schema = ?,
			table_schema = ?,
			nms = ?,
			last_row_count = ?
		WHERE name = ? AND id = ?`
		_, err := nmsDB.Exec(updateQuery, pgTableWithNMS.schema, pgTableWithNMS.tableSchema, pgTableWithNMS.nmsTime, pgTableWithNMS.rowCount, pgTableWithNMS.name, pgTableWithNMS.dsnEnum)
		if err != nil {
			return fmt.Errorf("seednmstable() update: %v", err)
		}
	} else {
		insertQuery := `
		INSERT INTO nmstables 
		(name, schema, table_schema, nmsColumn, nms, last_row_count, dsn) 
		VALUES (?, ?, ?, ?, ?, ?, ?)`
		_, err := nmsDB.Exec(insertQuery, pgTableWithNMS.name, pgTableWithNMS.schema, pgTableWithNMS.tableSchema, pgTableWithNMS.nmsColumn, pgTableWithNMS.nmsTime, pgTableWithNMS.rowCount, pgTableWithNMS.dsnEnum)
		if err != nil {
			return fmt.Errorf("seednmstable() insert: %v", err)
		}
	}

	return nil
}

func nmsTablesQuery(nmsDB *sql.DB, fileWrite bool) ([]table, error) {
	var tables []table

	rows, err := nmsDB.Query("SELECT id, name, schema, table_schema, bq_schema, nms, nmsColumn, last_row_count, dsn, last_shoved_on FROM nmstables")
	if err != nil {
		return nil, fmt.Errorf("nmsQuery select error: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		var schema string
		var tableSchema sql.NullString
		var bqSchema sql.NullString
		var nms time.Time
		var nmsColumn sql.NullString
		var rowCount int64
		var dsn int64
		var lastShove time.Time
		var t table
		err = rows.Scan(&id, &name, &schema, &tableSchema, &bqSchema, &nms, &nmsColumn, &rowCount, &dsn, &lastShove)
		if err != nil {
			return nil, fmt.Errorf("gettableswithnms() scan error: %v", err)
		}
		t.ID = id
		t.Name = name
		t.Schema = schema
		t.TableSchema = tableSchema.String
		t.BQSchema = bqSchema.String
		t.NMS = nms
		t.NMSColumn = nmsColumn.String
		t.LastRowCount = rowCount
		t.DSNEnum = dsn
		t.LastShove = lastShove
		tables = append(tables, t)
	}

	if fileWrite {
		err = seedStateBackup(tables)
		if err != nil {
			log.Printf("seedStateBackup(): %v", err)
		}
	}

	return tables, nil
}

func seedStateBackup(tables []table) error {
	f, err := os.OpenFile("./sqlite/seed_state.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()

	for _, t := range tables {
		byteArray, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("seedstatebackup json.marshal error:%v", err)
		}
		if _, err := fmt.Fprintf(f, "%s\n", byteArray); err != nil {
			return fmt.Errorf("seed_state.json file write error:%v", err)
		}
	}
	return nil
}

func updateNMS(t table, nmsDB *sql.DB) error {
	updateQuery := `
	UPDATE nmstables
	SET 
		nms = ?,
		last_row_count = ?,
		last_shoved_on = datetime('now')
	WHERE id = ?`

	_, err := nmsDB.Exec(updateQuery, t.NewNMS, t.LastRowCount, t.ID)
	if err != nil {
		return fmt.Errorf("updateNMS() exec error: %v", err)
	}
	return nil
}

func updateCachedBQSchema(tableID int, nmsDB *sql.DB, bqSchema bigquery.Schema) error {
	bqs, err := bqSchema.ToJSONFields()
	if err != nil {
		return fmt.Errorf("bqschema.tojsonfields error: %v", err)
	}

	updateQuery := `
	UPDATE nmstables
	SET 
		bq_schema = ?
	WHERE id = ?`

	_, err = nmsDB.Exec(updateQuery, string(bqs), tableID)
	if err != nil {
		return fmt.Errorf("updateNMS() exec error: %v", err)
	}
	return nil
}
