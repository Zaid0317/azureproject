@dlt.table
def dimtrack_stg():
    df= spark.readStream.table('spotify_cat.silver.dimtrack')
    return df
# this is called eclarative framework, that is why it is called declarative pipeline
# no need to worry abt how u need to do, u just need to tell what u need to do


dlt.create_streaming_table("dimtrack")

dlt.create_auto_cdc_flow(
  target = "dimtrack",
  source = "dimtrack_stg",
  keys = ["track_id"],
  sequence_by = "updated_at",
  system_sequence_by = None, # optional
  ignore_null_updates = False, # optional
  ignore_null_updates_column_list = None, # optional
  ignore_null_updates_except_column_list = None, # optional
  columns_to_update = None, # optional
  apply_as_deletes = None, # optional
  apply_as_truncates = None, # optional
  column_list = None, # optional
  except_column_list = None, # optional
  stored_as_scd_type = 2, # optional
  track_history_column_list = None, # optional
  track_history_except_column_list = None, # optional
  name = None, # optional
  once = False # optional
)