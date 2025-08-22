# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "8f68cf80-dc8d-44f9-ad64-3bf87dd468fc",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import time 
start_time = time.time()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Pipeline configurable params

# PARAMETERS CELL ********************

OrchestrationEnvironment = "" # prod or dev
partition_column = "" # "RunDateId"
deployment_name = "" # "gpt-35-turbo-16k"
suffix = "" # e.g. "prod"
test = "" # True or False, for main PL, this should be set as False
batch_size = ""
experiment_name = "" # experiment name to log metrics/artifacts/etc
runDateStr = ""  #run date yyyyMMdd used for Experiment logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# partition_column = "RunDateId"
# deployment_name = "gpt-4o"
# suffix = "prod"
# test = True
# batch_size = 1000
# experiment_name = "fitgai_logs"
# OrchestrationEnvironment = "dev"
# runDateStr = "20250611"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Run utils

# CELL ********************

external_deps = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run /Util_FITGAI 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ws_id_fitgai = lakehouse_fitgai_connection[f"{OrchestrationEnvironment}_buffer_fitgai_ws_id"]
lh_id_fitgai = lakehouse_fitgai_connection[f"{OrchestrationEnvironment}_buffer_fitgai_lh_id"]
schema_fitgai = lakehouse_fitgai_connection['buffer_schema']
print('ws_id_fitgai', ws_id_fitgai, 'lh_id_fitgai', lh_id_fitgai, 'schema_fitgai', schema_fitgai)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

default_segmentation_table = "segmented" # TODO: use this in report PL and then delete "FACT_Survey_Segmented"
dim_distinct_table_name = "DIM_Distinct_Responses"
not_processed_segmentation_table = "segmentation_not_processed"

temp_suffix = "_buffer_" + runDateStr # suffix for temporary fitgai tables

destination_table_name_segmentation = default_segmentation_table + temp_suffix + "_" + suffix # only add to temp tables
print("destination table name is :", destination_table_name_segmentation)
not_processed_segmentation_table = not_processed_segmentation_table + "_" + suffix
dim_d_r_name = dim_distinct_table_name + "_" + suffix

segmentation_caching_enabled = segmentation_caching_config.get(OrchestrationEnvironment,{}).get("enabled")
if segmentation_caching_enabled:
    segmentation_cache_ws_id = segmentation_caching_config[OrchestrationEnvironment]["ws_id"]
    segmentation_cache_lh_id = segmentation_caching_config[OrchestrationEnvironment]["lh_id"]
    segmentation_schema = segmentation_caching_config[OrchestrationEnvironment]["schema"]
    segmentation_cache_table_name = "DIM_FITGAI_Segmentation_Cache" + "_" + OrchestrationEnvironment

no_data = False # instantiate, and overwrite later, if condition is met

# TODO: make this explicit, system_message_prompt is the global variable used in the backend, controlled externally
system_message_prompt = SEGMENTATION_SYSTEM_PROMPT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Other params

# CELL ********************

# TODO: here we have to read from not processed tables to decide what to filter. ie if record was not processed due to RAI,
    # we don't attemp again, but if it was due to timeout, we attempt again
    
# data to be processed
df_survey = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, dim_d_r_name).readDF().filter("ProcessingDate is NULL").cache()
df_survey = df_survey.sort("LastResponseDate", "ResponseCount", ascending=[False, False])

df_count_raw = df_survey.count()

if batch_size is not None:
    # here we are limiting dataframe based on batch_size to conrol traffic on GAI
    df_survey = df_survey.limit(int(batch_size)).cache()
if test:
    display(df_survey)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare QC check after intake dedupe
df_count = df_survey.count()
if df_count==0:
    no_data = True
print(no_data, df_count)

# TODO: here we should log # of responses being processed in this run


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    df = df_survey.select("SurveyComment")
    df = df.withColumn("SurveyComment", col("SurveyComment").cast(StringType()))
    df_in_count = df.count()
    print(df_in_count, df_count)
    assert df_in_count==df_count
    if test:
        display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if segmentation_caching_enabled and not no_data:
    cols_DIM_Segmentation = ["SurveyComment", "Segment", "SegmentNumber","RunDateId","Coverage", "CumulativeCoverage"]
    
    schema_segmentation_cache = StructType([
        StructField("SurveyComment", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("SegmentNumber", IntegerType(), True),
        StructField("RunDateId", StringType(), True), 
        StructField("Coverage", DoubleType(), True),
        StructField("CumulativeCoverage", DoubleType(), True),
    ])
    
    # read DIM_Segmentation table if it exists, if not make an empty RDD
    file_list = mssparkutils.fs.ls(LakeHouseIOUtils(segmentation_cache_ws_id, segmentation_cache_lh_id, segmentation_schema).path)
    segmentation_cache_exists = any(item.name == segmentation_cache_table_name for item in file_list)
    # Load the full segmentation cache (all verbatims that have processed previously by fitgai)
    if segmentation_cache_exists:
        df_segmentation_cache = LakeHouseIOUtils(segmentation_cache_ws_id, segmentation_cache_lh_id, segmentation_schema, segmentation_cache_table_name).readDF()
        df_segmentation_cache = df_segmentation_cache.select(*cols_DIM_Segmentation).cache()
        df_segmentation_cache_count = df_segmentation_cache.count()
    else:
        print(f"Table: {segmentation_cache_table_name} does not exist, creating empty.")
        df_segmentation_cache_count = 0
        df_segmentation_cache = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_segmentation_cache)
    
    print(f"Segmentation cache has {df_segmentation_cache_count} rows.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if segmentation_caching_enabled and not no_data:
    # the rows that are present in DIM_segmentation are already segmented so we union df_segmentation_cache_batch to df_validated at the end.
    df_segmentation_cache_batch = df_segmentation_cache.join(df, ["SurveyComment"], how="left_semi") # join to get the rows in the current input that are present in the cache
    df_cached_segments_count = df_segmentation_cache_batch.count()
    no_cached_segment_data = df_cached_segments_count == 0

    # df should contain only those rows NOT in DIM_segmentation, so they can be segmented by LLM call.
    df = df.join(df_segmentation_cache, ["SurveyComment"], how='left_anti')  # this df now contains rows that are new (not found in cache), which will be segmented by LLM
    df_count = df.count()
    
    # All the data in the batch could be coming from the cache
    no_new_data = df.count() == 0
    print(df_count,df_cached_segments_count,df_in_count)
    assert (df_count + df_segmentation_cache_batch.select("SurveyComment").distinct().count()) == df_in_count
else:
    no_new_data = no_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_new_data:  # no_new_data flag is true if there is no data that needs to be segmented by the llm
    df_gpt_in = Preprocessing.segmentation(df)
    df_gpt_in_count = df_gpt_in.count()
    print(df_gpt_in_count, df_count)
    assert df_gpt_in_count==df_count
    if test:
        display(df_gpt_in)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_new_data:
    original_text_col = "SurveyComment"
    gpt_message_col = "message"
    # TODO: get token counts here
    chat_completion = (
        OpenAIChatCompletion()
        .setDeploymentName(deployment_name) # deployment name could be `gpt-35-turbo` or `gpt-35-turbo-16k`.
        .setTemperature(1.0) # it appears this impacts the content filtering, default (1.0), range 0.0 (always filtered)-2.0 (not filtered)
        .setMessagesCol(gpt_message_col)
        .setErrorCol("error")
        .setOutputCol("chat_completions")
    )
    df_gpt_out = chat_completion.transform(df_gpt_in).select(original_text_col, \
                                                             "error", \
                                                             f"chat_completions.choices.{gpt_message_col}.content", \
                                                             "chat_completions.choices.finish_reason", \
                                                             "chat_completions.id", \
                                                             "chat_completions.created").cache()
    gpt_out_count = df_gpt_out.count()
    print(df_gpt_in_count, gpt_out_count)
    assert df_gpt_in_count==gpt_out_count
    if test:
        display(df_gpt_out)
else:
    gpt_out_count = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_new_data:
    # find rows where gpt response was Null and there was a non Null value for the 
    #   error and cross examine with errors
    err_df = df_gpt_out.filter("content is NULL AND error is not NULL").cache()
    print('error')
    err_df_count = err_df.count()
    print('error count', err_df_count)
    if err_df_count!=0:
        display(err_df)

    not_finished_df = df_gpt_out.filter((df_gpt_out["finish_reason"][0] != "stop") & (df_gpt_out["finish_reason"][0] != "length")).cache()
    not_finished_df_count = not_finished_df.count()
    print('not finished count', not_finished_df_count)
    if not_finished_df_count!=0:
        display(not_finished_df)

    # gather those responses, as a list, where gpt produced null values and there was an error
    responses_with_error = err_df.select("SurveyComment").rdd.flatMap(lambda x: x).collect()
    responses_with_error = list(set(responses_with_error))
    print(len(responses_with_error))

    # gather those responses, as a list, where gpt produced null values and there was no error, finish reason was not stop or length
    responses_not_finished = not_finished_df.select("SurveyComment").rdd.flatMap(lambda x: x).collect()
    responses_not_finished = list(set(responses_not_finished))
    print(len(responses_not_finished))

    # let's remove these responses from df_gpt_out
    responses_not_processed = responses_with_error
    responses_not_processed.extend(responses_not_finished)

    # let's drop null values for content
    #   here, we are covering two scenarios where content from GAI is Null
    #       1) content is Null and there ishas an associated error, we should be good. And this is an assumption
    #       2) content is Null, there is no error, but finish reason is not stop/length
    # df_gpt_out = df_gpt_out.select("SurveyComment", "content").na.drop().cache()
    df_gpt_out = df_gpt_out.select("SurveyComment", "content").filter(~df_gpt_out.SurveyComment.isin(responses_not_processed)).cache()

    display(df_gpt_out)
    # Overwrite gpt_out_count
    gpt_out_count = df_gpt_out.count()
    print(gpt_out_count)
else:
    not_finished_df_count = 0
    err_df_count = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If LLM returns 0 segmented rows, override no_new_data to True
if not no_new_data and gpt_out_count==0:
    no_new_data = True

    # If LLM returns 0 segmented rows, but segmentation caching is enabled, we may still append rows and no_data should be False
    # Thus, only set no_data to True if segmentation caching is not enabled
    if not segmentation_caching_enabled:
        no_data = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(no_new_data)
print(no_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_new_data:
    post_processed = Postprocessing.segmentation(df_gpt_out)
    post_processed_count = post_processed.count()
    display(post_processed)
    print(df_in_count, df_gpt_in_count, gpt_out_count, post_processed_count)
    assert gpt_out_count<=post_processed_count
    print('postprocessing for segmentation done, ie flattening')
else:
    post_processed_count = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_new_data:
    df_validated = validate_gpt_output(post_processed, runDateStr, partition_column)
    count_before_null = df_validated.count()    
    print(count_before_null)
    if test:
        display(df_validated)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if segmentation_caching_enabled and not no_data:
    if not no_new_data: # there is new data that was segmented by LLM
        # Get all the rows that are newly segmented (this join creates a copy)
        df_segmentation_cache_new_entries = df_validated.join(df_segmentation_cache, ["SurveyComment"], how="left_anti")
        # add rows from the cache into df_validated
        df_validated = df_validated.union(df_segmentation_cache_batch)
        # add all the newly segmented rows not in cache
        df_segmentation_cache = df_segmentation_cache.union(df_segmentation_cache_new_entries)

        if test:
            display(df_segmentation_cache_batch.limit(5))
            display(df_segmentation_cache_new_entries.limit(5))
            print(f"Adding {df_cached_segments_count} entries from the segmentation cache.")
            print(f"Adding {df_segmentation_cache_new_entries.count()} entries to the segmentation cache.")
    # there is data in the cache but no new data from LLM
    elif not no_cached_segment_data:  
        df_validated = df_segmentation_cache_batch
    else:
        no_data = True   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    # lets find rows in df_validated that are duplicates and returning a new DataFrame containing only those duplicated rows.
    dupes = get_duplicates(df_validated)
    qc_count = dupes.count()
    print(qc_count)
    if qc_count!=0:
        display(dupes)
    assert qc_count==0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TODO: here we have to write to dim d table: # of tokens

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Store segmented responses
if not no_data:
    LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, schema_fitgai, destination_table_name_segmentation).writeDF(df_validated, \
                partition_column=partition_column, \
                replace_column=partition_column, \
                replace_value=runDateStr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# new data means rows were added to the segmentation cache
if segmentation_caching_enabled and not no_new_data:
    LakeHouseIOUtils(segmentation_cache_ws_id, segmentation_cache_lh_id, segmentation_schema, segmentation_cache_table_name).writeDF(df_segmentation_cache, \
                        partition_column=None, \
                        replace_column=None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Store error details
if not no_new_data and (not_finished_df_count!=0 or err_df_count!=0):
    # concat not_finished_df & err_df and store
    not_processed_df = err_df.union(not_finished_df)
    not_processed_df = not_processed_df.withColumn(partition_column, lit(runDateStr))
    display(not_processed_df)
    # TODO: write into a separate lh for production solution    
    LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, not_processed_segmentation_table).writeDF(not_processed_df, \
                partition_column=partition_column, \
                replace_column=partition_column, \
                replace_value=runDateStr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

end_time = time.time()
time_elapsed_sec = end_time - start_time # sec

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    runs = mlflow.search_runs(experiment_names=[experiment_name])
    runs = runs.sort_values(by=["start_time", "end_time"], ascending=[False, False])
    runs
except Exception as e:
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# is this only one, if not, pick the is latest
try:
    run_ids = runs[runs['tags.mlflow.runName']==runDateStr]["run_id"].values
    print(run_ids)
    run_id = run_ids[0]
    print(run_id)
except Exception as e:
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    experiment = mlflow.set_experiment(experiment_name)
    run = mlflow.start_run(run_id=run_id)
    mlflow.log_metric('Segmentation: Batch size', batch_size)
    mlflow.log_metric('Segmentation: Total non-processed records found in DIM distinct table', df_count_raw)
    mlflow.log_metric('Segmentation: Total records staged for processing for this run', df_count)
    mlflow.log_metric('Segmentation: Total records used from segmentation cache', df_cached_segments_count)
    mlflow.log_metric('Segmentation: Total records with GPT errors', err_df_count)
    mlflow.log_metric('Segmentation: Total records with GPT non-finished', not_finished_df_count)
    mlflow.log_metric('Segmentation: Total GPT processed records', gpt_out_count)
    mlflow.log_metric('Segmentation: Total segemnted records for this run', post_processed_count)
    mlflow.log_metric('Segmentation: Run time (sec)', time_elapsed_sec)
    mlflow.log_param("Deployment Name", deployment_name)
    mlflow.end_run()
except Exception as e:
    print('no logs collected')
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if no_data: # this is to handle skipping of fitgai PL steps, in case of no new data
    mssparkutils.notebook.exit(11)
elif not no_data:
    mssparkutils.notebook.exit(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
