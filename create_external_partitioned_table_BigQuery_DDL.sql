CREATE EXTERNAL TABLE IF NOT EXISTS PROJ_ID.SCHEMA_NAME.TABLE_NAME 
WITH PARTITION COLUMNS ( year INT64 )
OPTIONS(
      format = 'PARQUET',
      URIs = ['gs://bucket/path/*'],
	  hive_partition_uri_prefix = 'gs://bucket/path',
	  require_hive_partition_filter = false)
	);