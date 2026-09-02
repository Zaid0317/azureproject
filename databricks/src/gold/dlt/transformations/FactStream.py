@dlt.table

def factstream_stg():
    df= spark.readStream.table('spotify_cat.silver.factstream')
    return df
# this is called eclarative framework, that is why it is called declarative pipeline
# no need to worry abt how u need to do, u just need to tell what u need to do


dlt.create_streaming_table("factstream")

dlt.create_auto_cdc_flow(
  target = "factstream",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  stored_as_scd_type = 1,
  system_sequence_by = None, # optional
  ignore_null_updates = False, # optional
  ignore_null_updates_column_list = None, # optional
  ignore_null_updates_except_column_list = None, # optional
  columns_to_update = None, # optional
  apply_as_deletes = None, # optional
  apply_as_truncates = None, # optional
  column_list = None, # optional
  except_column_list = None, # optional
  track_history_column_list = None, # optional
  track_history_except_column_list = None, # optional
  name = None, # optional
  once = False # optional
)