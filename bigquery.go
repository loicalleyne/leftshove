package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/Jeffail/gabs/v2"
	"github.com/spf13/cast"
	"google.golang.org/api/googleapi"
)

func createBQtables() error {
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
		return fmt.Errorf("nmsdbopen error: %v", err)
	}
	tables, err := nmsTablesQuery(nmsDB, false)
	if err != nil {
		return fmt.Errorf("nmstablesquery error: %v", err)
	}

	projectID := os.Getenv("BQ_PROJECT")
	ctx := context.Background()
	bigqueryClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.newclient() error: %v", err)
	}
	defer bigqueryClient.Close()

	for i := 1; i <= int(dsnCount); i++ {
		datasetID := os.Getenv("BQ_DATASET_" + cast.ToString(i))
		dExists, err := checkDatasetExists(datasetID, bigqueryClient)
		if err != nil {
			log.Printf("checkdatasetexists() error: %v", err)
			continue
			// return fmt.Errorf("checkdatasetexists() error: %v", err)
		}
		if dExists {
			continue
		} else {
			err = createDataset(datasetID, bigqueryClient)
			if err != nil {
				return fmt.Errorf("checkdataset() error: %v", err)
			}
		}
	}

	for _, t := range tables {
		if t.TableSchema == "" {
			log.Printf("createbqtables() table(%v) %v - no tableschema", t.ID, t.Name)
			continue
		}
		datasetID := os.Getenv("BQ_DATASET_" + cast.ToString(t.DSNEnum))
		bqTableName := t.Name + "_cdc"
		tExists, tSchema, err := checkTableExists(datasetID, bqTableName, bigqueryClient)
		if err != nil {
			log.Printf("checktableexists():%v", err)
		}

		if !tExists {
			_, bqSchema, err := pgSchemaToBqSchema(t.TableSchema)
			if err != nil {
				return fmt.Errorf("pgschematobqschema() error: %v", err)
			}

			err = createBigQueryTableWithSchema(datasetID, bqTableName, bigqueryClient, bqSchema)
			if err != nil {
				return fmt.Errorf("createbigquerytableWithschema() error: %v", err)
			}
			err = updateCachedBQSchema(t.ID, nmsDB, bqSchema)
			if err != nil {
				return fmt.Errorf("updatecachedbqschema() error: %v", err)
			}

		} else {
			pgBQSchemaBytes, _, err := pgSchemaToBqSchema(t.TableSchema)
			if err != nil {
				return fmt.Errorf("pgschematobqschema() error: %v", err)
			}
			unchanged := compareBQSchemas(pgBQSchemaBytes, tSchema)
			log.Printf("BigQuery table %v:%v.%v exists, unchanged = %v", projectID, datasetID, t.Name, unchanged)
			// to do: add new fields to bq table if any
		}
		vExists, _, err := checkTableExists(datasetID, bqTableName, bigqueryClient)
		if err != nil {
			log.Printf("checktableexists():%v", err)
		}
		if !vExists && t.PKeyColumn != "" {
			err = createBigQueryPKeyView(datasetID, bqTableName, t.PKeyColumn, bigqueryClient)
			if err != nil {
				return fmt.Errorf("createbigquerypkeyview() error: %v", err)
			}
		}
	}
	return nil
}

func compareBQSchemas(cachedSchema, tableSchema []byte) bool {
	var cs map[string]interface{}
	var ts map[string]interface{}
	json.Unmarshal(cachedSchema, &cs)
	json.Unmarshal(tableSchema, &ts)
	return reflect.DeepEqual(cs, ts)
}

func checkDatasetExists(datasetID string, client *bigquery.Client) (bool, error) {
	ctx := context.Background()
	dataset := client.DatasetInProject(client.Project(), datasetID)
	if _, err := dataset.Metadata(ctx); err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
			return false, fmt.Errorf("dataset does not exist: %v", datasetID)
		} else {
			return false, fmt.Errorf("error checking dataset existence: %w", err)
		}
	} else {
		return true, nil
	}
}

func createDataset(datasetID string, client *bigquery.Client) error {
	ctx := context.Background()
	location := os.Getenv("BQ_LOCATION")
	if location == "" {
		location = "US" // Default to US if unspecified
	}
	meta := &bigquery.DatasetMetadata{
		Location: location, // See https://cloud.google.com/bigquery/docs/locations
	}
	if err := client.Dataset(datasetID).Create(ctx, meta); err != nil {
		return err
	}
	return nil
}

func checkTableExists(datasetID, tableID string, client *bigquery.Client) (bool, []byte, error) {
	ctx := context.Background()
	md, err := client.Dataset(datasetID).Table(tableID).Metadata(ctx)
	if hasStatusCode(err, http.StatusNotFound) {
		return false, nil, fmt.Errorf("table %v not found in dataset %v: %v", tableID, datasetID, err)
	}
	_, mdTableID, _ := strings.Cut(md.FullID, ".")
	if mdTableID == tableID {
		schemaBytes, err := md.Schema.ToJSONFields()
		if err != nil {
			return true, nil, fmt.Errorf("schema.tojsonfields() error:%v", err)
		}
		return true, schemaBytes, nil
	}
	return false, nil, nil
}

func createBigQueryTableWithSchema(datasetID, tableID string, client *bigquery.Client, schema bigquery.Schema) error {
	ctx := context.Background()

	metaData := &bigquery.TableMetadata{
		Schema: schema,
	}
	// to do: add partitioning options

	tableRef := client.Dataset(datasetID).Table(tableID)
	if err := tableRef.Create(ctx, metaData); err != nil {
		if hasStatusCode(err, http.StatusConflict) {
			return nil
		}
		return fmt.Errorf("createbigquerytablewithschema.tableref.create table %v: %v", tableID, err)
	}
	return nil
}

func createBigQueryPKeyView(datasetID, tableID, pKeyColumn string, client *bigquery.Client) error {
	ctx := context.Background()
	tableFullID := client.Project() + "." + "." + datasetID + "." + tableID
	viewQuery := "SELECT * FROM `{tableFullID}` WHERE ( {pkey} , snapshot_tm ) in (SELECT ({pkey}, max(snapshot_tm)) FROM `{tableFullID}` GROUP BY {pkey})"
	viewQuery = strings.Replace(viewQuery, "{tableFullID}", tableFullID, 2)
	viewQuery = strings.Replace(viewQuery, "{pkey}", pKeyColumn, 3)

	metaData := &bigquery.TableMetadata{
		ViewQuery: viewQuery,
		Type:      bigquery.ViewTable,
	}

	tableRef := client.Dataset(datasetID).Table(tableID)
	if err := tableRef.Create(ctx, metaData); err != nil {
		if hasStatusCode(err, http.StatusConflict) {
			return nil
		}
		return fmt.Errorf("createbigquerytablewithschema.tableref.create table %v: %v", tableID, err)
	}
	return nil
}

func hasStatusCode(err error, code int) bool {
	// https://go.dev/src/net/http/status.go
	if e, ok := err.(*googleapi.Error); ok && e.Code == code {
		return true
	}
	return false
}

func importJSONExplicitSchema(projectID, datasetID, tableID string) error {
	// projectID := "my-project-id"
	// datasetID := "mydataset"
	// tableID := "mytable"
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	gcsRef := bigquery.NewGCSReference("gs://cloud-samples-data/bigquery/us-states/us-states.json")
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.Schema = bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "post_abbr", Type: bigquery.StringFieldType},
	}
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteEmpty

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil {
		return fmt.Errorf("job completed with error: %v", status.Err())
	}
	return nil
}

func pgSchemaToBqSchema(tableSchema string) ([]byte, bigquery.Schema, error) {
	var bqTableSchema bigquery.Schema
	jsonParsed, err := gabs.ParseJSON([]byte(tableSchema))
	if err != nil {
		return nil, nil, fmt.Errorf("pgschematobqschema() tableschema parsejson: %v", err)
	}
	// tableID := jsonParsed.Path("table_name").Data().(string)
	for _, column := range jsonParsed.S("columns").Children() {
		columnName := column.Path("column_name").Data().(string)
		columnType := column.Path("udt_name").Data().(string)
		// is_nullable := column.Path("is_nullable").Data().(string)
		// ordinal_position := column.Path("ordinal_position").Data().(string)
		// column_default := column.Path("column_default").Data().(string)
		// data_type := column.Path("data_type").Data().(string)
		// character_maximum_length := column.Path("character_maximum_length").Data().(string)
		// numeric_precision_radix := column.Path("numeric_precision_radix").Data().(string)
		// dtd_identifier := column.Path("dtd_identifier").Data().(string)
		// is_identity := column.Path("is_identity").Data().(string)

		var field bigquery.FieldSchema
		field.Name = columnName
		/*
			// Benthos currently sends postgres arrays as string, not JSON array, causing errors. Defaulting to BQ string type for PG source array types
				if columnType[0:1] == "_" || strings.HasSuffix(columnType, "vector") {
					field.Repeated = true
				}
		*/
		baseType := strings.TrimPrefix(columnType, "_")
		// fmt.Printf("%v.%v : %v\n", tableID, columnName, baseType)
		if columnType[0:1] == "_" || strings.HasSuffix(columnType, "vector") {
			field.Type = bigquery.StringFieldType
		} else {
			// types float(n)
			if strings.HasPrefix(baseType, "float") {
				field.Type = bigquery.NumericFieldType
				if column.Path("numeric_precision").Data() != nil && column.Path("numeric_precision").Data().(float64) != 0.0 {
					field.Precision = int64(column.Path("numeric_precision").Data().(float64))
				}
				if column.Path("numeric_scale").Data() != nil && column.Path("numeric_scale").Data().(float64) != 0.0 {
					field.Precision = int64(column.Path("numeric_scale").Data().(float64))
				}
			} else {
				// types int(n), int(n)vector
				if strings.HasPrefix(baseType, "int") && !strings.HasSuffix(baseType, "erval") {
					field.Type = bigquery.IntegerFieldType
				} else {
					switch baseType {
					case "abstime":
						field.Type = bigquery.DateTimeFieldType
					case "bool":
						field.Type = bigquery.BooleanFieldType
					case "bytea":
						field.Type = bigquery.BytesFieldType
					case "char":
						field.Type = bigquery.StringFieldType
					case "date":
						field.Type = bigquery.DateFieldType
					case "inet":
						field.Type = bigquery.StringFieldType
					case "interval":
						field.Type = bigquery.StringFieldType
					case "json":
						field.Type = bigquery.StringFieldType
					case "jsonb":
						field.Type = bigquery.StringFieldType
					case "ltree":
						field.Type = bigquery.StringFieldType
					case "name":
						field.Type = bigquery.StringFieldType
					case "numeric":
						field.Type = bigquery.NumericFieldType
						if column.Path("numeric_precision").Data() != nil && column.Path("numeric_precision").Data().(float64) != 0.0 {
							field.Precision = int64(column.Path("numeric_precision").Data().(float64))
						}
						if column.Path("numeric_scale").Data() != nil && column.Path("numeric_scale").Data().(float64) != 0.0 {
							field.Precision = int64(column.Path("numeric_scale").Data().(float64))
						}
					case "oid":
						field.Type = bigquery.IntegerFieldType
					case "oidvector":
						field.Type = bigquery.IntegerFieldType
					case "point":
						field.Type = bigquery.StringFieldType
					case "regproc":
						field.Type = bigquery.StringFieldType
					case "text":
						field.Type = bigquery.StringFieldType
					case "timestamp":
						field.Type = bigquery.TimestampFieldType
					case "timestamptz":
						field.Type = bigquery.TimestampFieldType
					case "varchar":
						field.Type = bigquery.StringFieldType
					case "xid":
						field.Type = bigquery.IntegerFieldType
					default:
						field.Type = bigquery.StringFieldType
					}
				}
			}
		}
		bqTableSchema = append(bqTableSchema, &field)
	}
	var snapshotTime bigquery.FieldSchema
	snapshotTime.Name = "snapshot_tm"
	snapshotTime.Type = bigquery.TimestampFieldType
	bqTableSchema = append(bqTableSchema, &snapshotTime)
	bqSchemaJSON, err := bqTableSchema.ToJSONFields()
	if err != nil {
		return nil, nil, fmt.Errorf("tojsonfields() error: err")
	}
	return bqSchemaJSON, bqTableSchema, nil
}

func updateTableAddColumn(projectID, datasetID, tableID string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	tableRef := client.Dataset(datasetID).Table(tableID)
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return err
	}
	newSchema := append(meta.Schema,
		&bigquery.FieldSchema{Name: "phone", Type: bigquery.StringFieldType},
	)
	update := bigquery.TableMetadataToUpdate{
		Schema: newSchema,
	}
	if _, err := tableRef.Update(ctx, update, meta.ETag); err != nil {
		return err
	}
	return nil
}
