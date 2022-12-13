package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/spf13/cast"

	// Import only pure Benthos components, switch with `components/all` for all
	// standard components.
	_ "github.com/benthosdev/benthos/v4/public/components/gcp"
	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	_ "github.com/benthosdev/benthos/v4/public/components/sql"
)

type benthosStreamConfig struct {
	inputYAML     string
	processorYAML string
	outputYAML    string
}

func newStream(dbURL string, t table) (*service.Stream, error) {
	conf, err := newStreamConfig(dbURL, t)
	if err != nil || conf.inputYAML == "" || conf.outputYAML == "" {
		return nil, fmt.Errorf("newstreamconfig() failed: %v : %v", t.Name, err)
	}

	builder := service.NewStreamBuilder()

	err = builder.AddInputYAML(conf.inputYAML)
	if err != nil {
		return nil, fmt.Errorf("addinputyaml failed: %v : %v", t.Name, err)
	}

	if conf.processorYAML != "" {
		err = builder.AddProcessorYAML(conf.processorYAML)
		if err != nil {
			return nil, fmt.Errorf("addprocessoryaml failed: %v : %v", t.Name, err)
		}
	}

	err = builder.AddOutputYAML(conf.outputYAML)
	if err != nil {
		return nil, fmt.Errorf("addoutputyaml failed: %v : %v", t.Name, err)
	}

	loggerYAML := `level: {logLevel}
format: json
add_timestamp: true
static_fields:
  '@service': benthos
file:
  path: ./leftshove.log
  rotate: true
  rotate_max_age_days: 14`
	loggerYAML = strings.Replace(loggerYAML, "{logLevel}", os.Getenv("BENTHOS_LOG_LEVEL"), 1)
	err = builder.SetLoggerYAML(loggerYAML)
	if err != nil {
		return nil, fmt.Errorf("setloggeryaml failed: %v : %v", t.Name, err)
	}

	// Build a stream with our configured components.
	stream, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("builder.build() failed: %v : %v", t.Name, err)
	}
	writeConfigFile(t, conf)
	return stream, nil
}

func newStreamConfig(dbURL string, t table) (benthosStreamConfig, error) {
	var conf benthosStreamConfig
	inputYAML := `sql_raw:
  driver: "postgres"
  dsn: "{dsn}"
  query: "{query}"`
	inputConf := strings.Replace(strings.Replace(inputYAML, "{dsn}", dbURL, 1), "{query}", t.Query, 1)
	conf.inputYAML = inputConf
	// err = builder.AddProcessorYAML(`bloblang: 'root = content().uppercase()'`)
	// panicOnErr(err)
	outputType := os.Getenv("OUTPUT_TYPE")
	switch outputType {
	case "BQ":
		projectID := os.Getenv("BQ_PROJECT")
		datasetID := os.Getenv("BQ_DATASET_" + cast.ToString(t.DSNEnum))
		if projectID != "" && datasetID != "" {
			conf.outputYAML = newBigQueryStreamConfig(t)
		} else {
			return conf, fmt.Errorf("missing bq configuration")
		}
	case "FILE":
		err := os.MkdirAll("output", 0755)
		if err != nil {
			fmt.Println(err)
		}
		outputYAML := `file:
  path: ./output/{tableName}.json
  codec: lines`
		outputYAML = strings.Replace(outputYAML, "{tableName}", t.Name, 1)
		conf.outputYAML = outputYAML
	}
	// log.Printf("conf: %+v", conf)
	return conf, nil
}

func newBigQueryStreamConfig(t table) string {
	projectID := os.Getenv("BQ_PROJECT")
	datasetID := os.Getenv("BQ_DATASET_" + cast.ToString(t.DSNEnum))
	batchCount := os.Getenv("BQ_BATCH_COUNT")
	if batchCount == "" {
		batchCount = cast.ToString(4096)
	}
	batchBytes := os.Getenv("BQ_BATCH_BYTES")
	if batchBytes == "" {
		batchBytes = cast.ToString(40000000)
	}
	batchPeriod := os.Getenv("BQ_BATCH_PERIOD")
	if batchPeriod == "" {
		batchPeriod = "20s"
	}
	outputYAML := `
gcp_bigquery:
  project: "{projectID}"
  dataset: "{datasetID}"
  table: "{tableID}"
  format: NEWLINE_DELIMITED_JSON
  max_in_flight: 64
  write_disposition: WRITE_APPEND
  create_disposition: CREATE_NEVER
  ignore_unknown_values: false
  max_bad_records: 0
  auto_detect: false
  batching:
    count: {batchCount}
    byte_size: {batchBytes}
    period: "{batchPeriod}"`
	outputConf := strings.Replace(strings.Replace(strings.Replace(strings.Replace(outputYAML, "{projectID}", projectID, 1), "{datasetID}", datasetID, 1), "{tableID}", t.Name+"_cdc", 1), "{batchCount}", batchCount, 1)
	outputConf = strings.Replace(strings.Replace(outputConf, "{batchBytes}", batchBytes, 1), "{batchPeriod}", batchPeriod, 1)
	return outputConf
}

func writeConfigFile(t table, conf benthosStreamConfig) {
	err := os.MkdirAll("stream_configs", 0755)
	if err != nil {
		fmt.Println(err)
	}
	f, err := os.OpenFile("./stream_configs/"+cast.ToString(t.DSNEnum)+"_"+t.Name+".json", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()

	if _, err := fmt.Fprintf(f, "%v\n%v\n%v", conf.inputYAML, conf.processorYAML, conf.outputYAML); err != nil {
		log.Printf("seed_state.json file write error:%v", err)
	}
}
