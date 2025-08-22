# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "71dac4d7-0682-419d-8e03-b832ed9ae891",
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
label_name = "" # e.g. "Sentiment", value must be upper case
partition_column = "" # e.g. "RunDateId"
deployment_name = "" # e.g. "gpt-35-turbo-16k" 
suffix = "" # e.g. "prod"
test = "" # True or False, for main PL, this should be set as False
experiment_name = ""
runDateStr = ""  #run date yyyyMMdd used for Experiment logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OrchestrationEnvironment = "dev" # prod or dev
# label_name = "Sentiment" 
# partition_column = "RunDateId"
# deployment_name = "gpt-35-turbo-16k" 
# suffix = "leonzha_debug" # e.g. "prod"
# test = True
# experiment_name = "FITGAI_exp"
# runDateStr = "20241009" #run date in format yyyyMMdd used for Experiment logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Run utils

# CELL ********************

external_deps = True

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

original_text_col = "SurveyComment"
segmented_text_col = "Segment"
original_label_col = label_name+"Label"
gpt_message_col = "message"
response_id_cols = [original_text_col, segmented_text_col, original_label_col]

no_data = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_distinct_table_name = "DIM_Distinct_Responses"
not_processed_validation_table = "gpt_validation_not_processed"

default_label_table = "labeled"  # TODO: use this in report PL and then delete "FACT_Survey_Labeled"
default_validation_folder = "validations" # TODO: use this in report PL and then delete "FACT_Validations"
temp_suffix = "_buffer_" + runDateStr # suffix for temporary fitgai tables

default_label_table += temp_suffix
default_validation_folder += temp_suffix

print("default table names, labeled: ", default_label_table, ", validation folders:", default_validation_folder)

dim_d_r_name = dim_distinct_table_name + "_" + suffix
destination_table_name_validation = default_validation_folder + "_" + suffix
# Hardcode suffix with one validation pass. Include iteration parameter and appropriate checks if adding multiple validation passes.
suffix += "_temp"
# If adding multiple validation passes, include iteration in parameter
# Suffix becomes _temp if iteration zero and should be fetching directly from labeling_postprocessing
# If additional iterations, change path away from labeling_postprocessing and toward validation output table

source_table_name_labeling = default_label_table + "_" + suffix

not_processed_validation_table = label_name + "_" + not_processed_validation_table + "_" + suffix

print("destination table:", destination_table_name_validation, "source table:", source_table_name_labeling)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert label_name in ["Sentiment", "Workload", "Artifact", "Platform"]

# overwrite for each case 
if label_name=="Sentiment":
    label_explain_list = None
    system_message_prompt_validation = SENTIMENT_VALIDATION_SYSTEM_PROMPT

if label_name=="Workload":
    label_explain_list = WORKLOAD_EXPLANATION_LIST
    system_message_prompt_validation = WORKLOAD_VALIDATION_SYSTEM_PROMPT

if label_name=="Artifact":
    label_explain_list = None
    system_message_prompt_validation = ARTIFACT_VALIDATION_SYSTEM_PROMPT #TODO DOES NOT EXIST YET

if label_name=="Platform":
    label_explain_list = PLATFORM_HORIZONTAL_EXPLANATION_LIST
    system_message_prompt_validation = PLATFORM_HORIZONTAL_VALIDATION_SYSTEM_PROMPT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TODO: here we have to read from not processed tables to decide what to filter. ie if record was not processed due to RAI,
    # we don't attemp again, but if it was due to timeout, we attempt again
    
# data to be processed
df_survey = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, dim_d_r_name).readDF().filter("ProcessingDate is NULL").select(original_text_col).cache() # TODO: this is not needed as we go with a temp table for each fitgai run
df_validation = LakeHouseIOUtils(ws_id=ws_id_fitgai, \
                            lh_id=lh_id_fitgai, \
                            schema_name=schema_fitgai, \
                            table_name=source_table_name_labeling, \
                            relative_path=None).readDF() # filter here

input_df = df_survey.join(df_validation, on=original_text_col, how="inner")
if test:
    display(input_df)
input_count = input_df.count()
print("total records read:" , input_count)
if input_count==0:
    no_data = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    # find  rows in input_df that are duplicates and returning a new DataFrame containing only those duplicated rows.
    dupes = get_duplicates(input_df)
    qc_count = dupes.count()
    print('dupes', qc_count)
    if qc_count!=0:
        display(dupes)
    assert qc_count==0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    df = input_df.select(*response_id_cols).cache()
    dupes = get_duplicates(df)
    qc_count = dupes.count()
    print('dupes', qc_count)
    if qc_count!=0:
        display(dupes)
    assert qc_count==0
    if test:
        display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Run validation module

# CELL ********************

if not no_data:
    df_gpt_in = Preprocessing.validation(df, original_label_col, label_name, system_message_prompt_validation)
    if test:
        display(df_gpt_in)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
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
                                                             segmented_text_col, \
                                                             original_label_col, \
                                                             "error", \
                                                             f"chat_completions.choices.{gpt_message_col}.content", \
                                                             "chat_completions.choices.finish_reason", \
                                                             "chat_completions.id", \
                                                             "chat_completions.created").cache()
    if test:
        display(df_gpt_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:

    # find rows where gpt response was Null and there was a non Null value for the 
    #   error and cross examine with errors
    err_df = df_gpt_out.filter("content is NULL AND error is not NULL").cache()
    err_df_count = err_df.count()
    print('error count', err_df_count)
    if err_df_count!=0:
        print('displaying err_df:')
        display(err_df)

    not_finished_df = df_gpt_out.filter((df_gpt_out["finish_reason"][0] != "stop") & (df_gpt_out["finish_reason"][0] != "length")).cache()
    not_finished_df_count = not_finished_df.count()
    print('not finished count', not_finished_df_count)
    if not_finished_df_count!=0:
        print('displaying not_finished_df:')
        display(not_finished_df)

    # gather those responses, as a list, where gpt produced null values and there was an error
    responses_with_error = err_df.select(original_text_col).rdd.flatMap(lambda x: x).collect()
    responses_with_error = list(set(responses_with_error))
    print('number of responses_with_error', len(responses_with_error))

    # gather those responses, as a list, where gpt produced null values and there was no error, finish reason was not stop or length
    responses_not_finished = not_finished_df.select(original_text_col).rdd.flatMap(lambda x: x).collect()
    responses_not_finished = list(set(responses_not_finished))
    print('number of responses_not_finished', len(responses_not_finished))

    # let's remove these responses from df_gpt_out
    responses_not_processed = responses_with_error
    responses_not_processed.extend(responses_not_finished)

    print('number of responses_not_processed', len(responses_not_processed))

    # let's drop null values for content
    #   here, we are covering two scenarios where content from GAI is Null
    #       1) content is Null and there ishas an associated error, we should be good. And this is an assumption
    #       2) content is Null, there is no error, but finish reason is not stop/length
    # df_gpt_out = df_gpt_out.select("SurveyComment", "content").na.drop().cache()
    display(df_gpt_out)
    drop_df = df_gpt_out.filter(df_gpt_out.SurveyComment.isin(responses_not_processed)).cache()
    rows_dropped = drop_df.count()
    print('number of rows droped', rows_dropped)
    df_gpt_out = df_gpt_out.filter(~df_gpt_out.SurveyComment.isin(responses_not_processed)).cache()
    display(df_gpt_out)
    # Overwrite gpt_out_count
    gpt_out_count = df_gpt_out.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    # Split result into Consensus and Justification KVP in a struct
    df_gpt_out = df_gpt_out.withColumn("temp_split_col", split(df_gpt_out["content"][0],"\n",2))
    
    df_gpt_out = df_gpt_out.withColumn("Consensus", df_gpt_out["temp_split_col"][0]) \
                           .withColumn("Justification", df_gpt_out["temp_split_col"][1])

    df_gpt_out = df_gpt_out.drop("content", "temp_split_col")

    # Next line is becasue I could not make it work to find Nulls in row, we can improve this step
    df_gpt_out = df_gpt_out.withColumn('Justification', when(col('Justification').isNull(), '-999') \
                           .otherwise(col('Justification')))
    
    # fix variations in the validation
    df_gpt_out = fix_validation_variations(df_gpt_out, original_label_col)

    # here after cleaning, i have to construct struct from Consensus and Justification 
    # and assign to content and drop Consensus and Justification 

    df_gpt_out = df_gpt_out.withColumn("content", \
        struct(df_gpt_out["Consensus"].alias("Consensus"),
               df_gpt_out["Justification"].alias("Justification")))

    df_gpt_out = df_gpt_out.drop("Consensus", "Justification")

    if test:
        display(df_gpt_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Attempt heuristic check for contradictions
if not no_data:
    #TODO leaving this here for potential expansion
    #this_label = df_gpt_out.select(original_label_col).distinct().rdd.flatMap(lambda x: x).collect()
    #TODO integration with fuzzywuzzy for key phrases? esp. "for this/the segment" type phrases
    heuristic_conditions = (
        (col("content.Consensus").contains("Disagree")) &
        (# Enumerate clauses for contradiction detection, one non-edge variable per clause for maximum coverage
            (col("content.Justification").like("%%is appropriate for %% segment%%")) | # the/this
            (col("content.Justification").like("%%is accurate for %% segment%%")) | # the/this 
            (col("content.Justification").like("%%I agree with %% label%%")) | # the/this & label value
            (col("content.Justification").like("%%the label %% is accurate%%")) # label value
        )
    )
    # Perform another pass overwriting content to contain new values based on heuristic
    df_gpt_out = df_gpt_out.withColumn("content", 
        struct( # Override consensus and justification if heuristic is True
            when(heuristic_conditions, "Agree").otherwise(col("content.Consensus")).alias("Consensus"),
            when(heuristic_conditions, "Heuristic detected contradiction, override to Agree.").otherwise(col("content.Justification")).alias("Justification")
        )
    )
    if test:
        display(df_gpt_out)
        #Retaining code for ease of debug selection of segments that have been altered
        #display(df_gpt_out.where(col("content.Consensus").contains("Agree") & col("temp_split_col")[0].contains("Disagree")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Clean up df_gpt_out before proceeding
if not no_data:
    df_gpt_out = df_gpt_out.withColumnRenamed("content", label_name+"Label_validation").cache()
    df_gpt_out = df_gpt_out.drop("error") # Dropping error to avoid Nulls in the next step
    display(df_gpt_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    gpt_out_count = df_gpt_out.count()
    print(input_count, gpt_out_count+rows_dropped, response_id_cols)
    assert input_count==gpt_out_count+rows_dropped


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Combine and Store

# CELL ********************

if not no_data:
    dupes = get_duplicates(df_gpt_out)
    qc_count = dupes.count()
    print('dupes', qc_count)
    if qc_count!=0:
        display(dupes)
    assert qc_count==0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    # first join input data to the output of gpt
    df_gpt_out_aug = Postprocessing.join(input_df, df_gpt_out, on=response_id_cols).cache()
    if test:
        display(df_gpt_out_aug)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    final_count = df_gpt_out_aug.count()
    print(final_count, gpt_out_count)
    assert final_count==gpt_out_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    relative_path = f"{runDateStr[0:4]}/{runDateStr[4:6]}/{runDateStr[6:]}/{label_name}"+"_validation"

    LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, destination_table_name_validation, relative_path).writeDF(df_gpt_out_aug, \
                        partition_column=partition_column, \
                        replace_column=partition_column, \
                        replace_value=runDateStr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data and (not_finished_df_count!=0 or err_df_count!=0):
    # concat not_finished_df & err_df and store
    not_processed_df = err_df.union(not_finished_df)
    not_processed_df = not_processed_df.withColumn(partition_column, lit(runDateStr))
    display(not_processed_df)
    
    LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, not_processed_validation_table).writeDF(not_processed_df, \
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
    mlflow.log_metric(f"{label_name} Validation total records for this job", input_count)
    mlflow.log_metric(f"{label_name} Validation GPT errors count", err_df_count)
    mlflow.log_metric(f"{label_name} Validation GPT not finished count", not_finished_df_count)
    mlflow.log_metric(f"{label_name} Validation GPT processed count", gpt_out_count)
    mlflow.log_metric(f"{label_name} Validation final count", final_count)
    mlflow.log_metric(f"{label_name} Validation Run time (sec)", time_elapsed_sec)
    mlflow.end_run()
except Exception as e:
    print('no logs collected')
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
