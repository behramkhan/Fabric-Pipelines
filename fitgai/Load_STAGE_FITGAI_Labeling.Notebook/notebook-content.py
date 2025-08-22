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
label_name = "" # e.g. "Sentiment", value needs to be uppercase 
partition_column = "" #e.g. "RunDateId"
deployment_name = "" # e.g. "gpt-35-turbo-16k" 
suffix =  "" # e.g. "prod"
iteration = "" # e.g 0
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
# suffix = "e2e"
# iteration = 0
# test = True
# experiment_name = "fitgai_prod_like_2"
# runDateStr = "20241009" #run date in format yyyyMMdd used for Experiment logging

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
print('ws_id_fitgai', ws_id_fitgai, 'lh_id_fitgai', lh_id_fitgai, 'schema_fitgai',schema_fitgai)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

original_text_col = "SurveyComment"
segmented_text_col = "Segment"
gpt_message_col = "message"
response_id_cols = [original_text_col, segmented_text_col]

no_data = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

default_segmentation_table = "segmented" # TODO: use this in report PL and then delete "FACT_Survey_Segmented"
default_validation_table = "validated" # TODO:  use this in report PL and then delete "FACT_Survey_Validated"
default_label_folder = "labels" # TODO:  use this in report PL and then delete  "FACT_Labels"
temp_suffix = "_buffer_" + runDateStr # suffix for temporary fitgai tables

default_segmentation_table += temp_suffix
default_validation_table += temp_suffix
default_label_folder += temp_suffix

print("default table names, segmentation: ", default_segmentation_table, ", validation:", default_validation_table, ", label folders:", default_label_folder)

print("iteration is: ", iteration)
dim_distinct_table_name = "DIM_Distinct_Responses"
not_processed_labeling_table = "labeling_not_processed"

dim_d_r_name = dim_distinct_table_name + "_" + suffix
if iteration == 0:
    source_table_name_segmentation = default_segmentation_table + "_" + suffix
    source_table = source_table_name_segmentation
    not_processed_labeling_table = label_name + "_" + not_processed_labeling_table + "_" + suffix + "_iteration0"
elif iteration == 1:
    source_table_name_validation = default_validation_table + "_" + suffix
    source_table = source_table_name_validation
    not_processed_labeling_table = label_name + "_" + not_processed_labeling_table + "_" + suffix + "_iteration1"

print("source table:", source_table)

if iteration == 0: # only for destination
    suffix += "_temp"
destination_table_name_labeling = default_label_folder + "_" + suffix

print("destination table:", destination_table_name_labeling)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert label_name in ["Sentiment", "Workload", "Artifact", "Platform", "Support"]

# overwrite for each case 
if label_name=="Sentiment":
    label_list = SENTIMENT_TYPE_LIST 
    label_explain_list = None
    system_message_prompt_label = SENTIMENT_ASSIGNMENT_SYSTEM_PROMPT

if label_name=="Workload":
    label_list = WORKLOAD_LIST
    label_explain_list = WORKLOAD_EXPLANATION_LIST
    system_message_prompt_label = WORKLOAD_ASSIGNMENT_SYSTEM_PROMPT

if label_name=="Artifact":
    label_list = ARTIFACT_LIST
    label_explain_list = ARTIFACT_EXPLANATION_LIST
    system_message_prompt_label = ARTIFACT_ASSIGNMENT_SYSTEM_PROMPT

if label_name=="Platform":
    label_list = PLATFORM_HORIZONTAL_LIST
    label_explain_list = PLATFORM_HORIZONTAL_EXPLANATION_LIST
    system_message_prompt_label = PLATFORM_HORIZONTAL_ASSIGNMENT_SYSTEM_PROMPT

if label_name=="Support":
    label_list = SUPPORT_LIST
    label_explain_list = SUPPORT_EXPLANATION_LIST
    system_message_prompt_label = SUPPORT_ASSIGNMENT_SYSTEM_PROMPT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TODO: here we have to read from not processed tables to decide what to filter. ie if record was not processed due to RAI,
    # we don't attemp again, but if it was due to timeout, we attempt again
    
# data to be processed
df_survey = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, dim_d_r_name).readDF().filter("ProcessingDate is NULL").select(original_text_col).cache() #TODO: this is not needed, as we will go with temp tables for each fitgai run
df_labels = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, source_table).readDF() # filter here

input_df = df_survey.join(df_labels, on=original_text_col, how="inner")
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

all_agree = False #TODO this is a temp place,can be improved
rem_count = 0
if iteration==1:
    input_df = input_df.withColumn(label_name+"_iteration", lit(0))

    input_df = input_df.withColumn('temp_validation', col(label_name+"Label_validation"))
    
    print(response_id_cols)
    response_id_cols.append("temp_validation")
    print(response_id_cols)
    
    cond = input_df.temp_validation.Consensus.isin(["Agree"])
    print(input_df.count())
    input_df_rem = input_df.filter(cond).cache()
    input_df = input_df.filter(~cond).cache()
    input_df = input_df.withColumn(label_name+"_iteration", lit(iteration))
    rem_count = input_df_rem.count()
    input_df_count = input_df.count()
    print(input_df_count, rem_count)

    if input_df_count==0:
        all_agree = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if (not no_data) and (not all_agree):
    print(response_id_cols)
    df = input_df.select(*response_id_cols).cache()
    dupes = get_duplicates(df)
    qc_count = dupes.count()
    print('duplicate counts', qc_count)

    if qc_count!=0:
        display(dupes)
    assert qc_count==0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Run labeling module

# CELL ********************

if (not no_data) and (not all_agree):
    df_gpt_in = Preprocessing.labeling(df, label_list, label_explain_list, label_name, system_message_prompt_label, iteration)
    if test:
        display(df_gpt_in)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if (not no_data) and (not all_agree):
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

if (not no_data) and (not all_agree):
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
else:
    gpt_out_count = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if (not no_data) and (not all_agree):
    df_gpt_out = Postprocessing.labeling_single_label(df_gpt_out)
    if test:
        display(df_gpt_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if (not no_data) and (not all_agree):
    # Label name already upper case, append for UpperCamelCase format
    df_gpt_out = df_gpt_out.withColumnRenamed("content", label_name+"Label").cache() 
    if test:
        display(df_gpt_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if (not no_data) and (not all_agree):
    gpt_out_count = df_gpt_out.count()
    print(input_count, gpt_out_count+rem_count+rows_dropped, response_id_cols)
    assert input_count==gpt_out_count+rem_count+rows_dropped


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Combine and Store

# CELL ********************

if (not no_data) and (not all_agree):
    dupes = get_duplicates(df_gpt_out)
    qc_count = dupes.count()
    print('dupe count', qc_count)
    if qc_count!=0:
        display(dupes)
    assert qc_count==0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if iteration==1: # to keep the label from iteration 0
    if not all_agree:
        input_df = input_df.withColumnRenamed(label_name+"Label", label_name+"Label_iteration0") # rename 
        input_df_rem = input_df_rem.withColumn(label_name+"Label_iteration0", col(label_name+"Label")) # copy

        response_id_cols.remove("temp_validation")
        print(response_id_cols)
    else:
        input_df_rem = input_df_rem.withColumn(label_name+"Label_iteration0", col(label_name+"Label")) # copy
        input_df_rem = input_df_rem.withColumn(label_name+"Label", col(label_name+"Label")) # copy

    display(input_df)
    display(input_df_rem)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    if not all_agree:
        # first join input data to the output of gpt
        df_gpt_out_aug = Postprocessing.join(input_df, df_gpt_out, on=response_id_cols).cache()
    else:
        df_gpt_out_aug = input_df_rem
    if test:
        display(df_gpt_out_aug)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if iteration==1:
    input_df_rem = input_df_rem.drop("temp_validation")
    if not all_agree:
        df_gpt_out_aug = df_gpt_out_aug.drop("temp_validation")
        assert set(input_df_rem.columns)==set(df_gpt_out_aug.columns) 
        df_gpt_out_aug = df_gpt_out_aug.select(*input_df_rem.columns)
        assert df_gpt_out_aug.dtypes == input_df_rem.dtypes
        df_gpt_out_aug = df_gpt_out_aug.union(input_df_rem) # here
    else:
        df_gpt_out_aug = input_df_rem

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    print(label_name)
    display(df_gpt_out_aug)
    final_count = df_gpt_out_aug.count()
    print(final_count, gpt_out_count)
    assert final_count==gpt_out_count+rem_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    df_gpt_out_aug = df_gpt_out_aug.sort(original_text_col, "SegmentNumber")
    _list =[original_text_col, "Segment", "SegmentNumber", label_name+"Label"] # Label name already upper case, append for UpperCamelCase format
    if iteration==1:
        _list.extend([label_name+"Label_iteration0", label_name+"_iteration"])
    display(df_gpt_out_aug.select(*_list))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    relative_path = f"{runDateStr[0:4]}/{runDateStr[4:6]}/{runDateStr[6:]}/{label_name}"
    LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, destination_table_name_labeling, relative_path).writeDF(df_gpt_out_aug, \
                        partition_column=partition_column, \
                        replace_column=partition_column, \
                        replace_value=runDateStr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if (not no_data) and (not all_agree) and (not_finished_df_count!=0 or err_df_count!=0) :
    # concat not_finished_df & err_df and store
    not_processed_df = err_df.union(not_finished_df)
    not_processed_df = not_processed_df.withColumn(partition_column, lit(runDateStr))
    display(not_processed_df)

    LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, not_processed_labeling_table).writeDF(not_processed_df, \
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
    mlflow.log_metric(f"{label_name} Labeling Iteration {iteration} total records for this job", input_count)
    mlflow.log_metric(f"{label_name} Labeling Iteration {iteration} GPT errors count", err_df_count)
    mlflow.log_metric(f"{label_name} Labeling Iteration {iteration} GPT not finished count", not_finished_df_count)
    mlflow.log_metric(f"{label_name} Labeling Iteration {iteration} GPT processed count", gpt_out_count)
    mlflow.log_metric(f"{label_name} Labeling Iteration {iteration} final count", final_count)

    if iteration==1:
        mlflow.log_metric(f"{label_name} Labeling Iteration {iteration} total records with Agreed Consensus", rem_count)
        mlflow.log_metric(f"{label_name} Labeling Iteration {iteration} total records with Disagreed Consensus", input_df_count)

    mlflow.log_metric(f"{label_name} Labeling Iteration {iteration} Run time (sec)", time_elapsed_sec)
    mlflow.end_run()
except Exception as e:
    print('no logs collected')
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
