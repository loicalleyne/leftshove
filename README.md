**leftshove** 
*why LEFT JOIN when you can LEFTSHOVE?*
An opinionated CDC utility.
Captures incremental table snapshots by querying on windows of 'not modified since' timestamps.
Uses Benthos Streams under the hood for streaming data from source to sink.
State stored locally with sqlite.

## Supported sources:
PostgreSQL

## Supported sinks:
BigQuery

### BigQuery:
- automatic creation of dataset and tables (requires GCP Application Default Credentials with appropriate permissions)

## Run:
```shell
./leftshove -config=./sample.env -seed -bq -cdc
```
### Arguments
- config: path to environment variable file (required)
- seed: connect to source database and automatically collect source table information (default: false)
- bq: automatically create dataset(s)/table(s) in BigQuery matching the source table schema with compatible types (default: false)
- cdc: run change data capture (default: false)
- runonce: interate source tables only once (default: false)

## To do:
- implement way to define exceptions for snapshot window field name (not_modified_since, nms, etc...); for now solution is to run mutation query in sqlite
- additional Benthos-supported outputs
- option for output to parquet file + S3/GCS
- option for output to BigQuery streaming/storage API insert
- option for output to any Benthos output
- handle source table name collisions; for now it is recommended to output each source to a separate BigQuery dataset
- fix Benthos logging to file