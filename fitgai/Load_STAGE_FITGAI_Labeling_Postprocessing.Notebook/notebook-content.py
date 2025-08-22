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
partition_column = "" # "RunDateId"
suffix = "" # e.g. "prod"
iteration = "" # e.g. 0
test = "" # True or False, for main PL, this should be set as False
experiment_name = ""
runDateStr = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OrchestrationEnvironment = "dev" # prod or dev
# partition_column = "RunDateId"
# suffix = "leonzha_debug" # e.g. "prod"
# iteration = 1
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

label_names = ["Sentiment", "Workload", "Platform"] #TODO improve
if iteration==0:
    label_names.append("Support")

no_data = False

_, runDateInt = produce_dates(runDateStr)
print("run date id is :", runDateInt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_distinct_table_name = "DIM_Distinct_Responses"

default_label_folder = "labels" # TODO: use this in report PL and then delete "FACT_Labels"
default_label_table = "labeled" # TODO: use this in report PL and then delete "FACT_Survey_Labeled"
temp_suffix = "_buffer_" + runDateStr # suffix for temporary fitgai tables

default_label_folder += temp_suffix
default_label_table += temp_suffix

print("default table names, label folders: ", default_label_folder, ", labeled:", default_label_table)
print("iteration is: ", iteration)

dim_d_r_name = dim_distinct_table_name + "_" + suffix
if iteration == 0:
    suffix += "_temp"
source_table_name_label = default_label_folder +  "_" + suffix
destination_table_name_label_postprocessed = default_label_table + "_" + suffix
print("source table: ", source_table_name_label, ", destination table:", destination_table_name_label_postprocessed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# we only load dim d in iteration one, so we can write back the ProcessingDate
if iteration ==1:
    # here we read the entire dim d table, without any filters, replace processing date for certain rows at the end, and overwrite to dim d
    df_survey = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, dim_d_r_name).readDF().cache()
    df_survey_count = df_survey.count()
    print(df_survey_count)

#TODO: can you change the sub-dir in labeling and use date filtering here?
shared_relative_path = f"{runDateStr[0:4]}/{runDateStr[4:6]}/{runDateStr[6:]}"

relative_path = f"{shared_relative_path}/{label_names[0]}" # for sentiment
sentiment_df = LakeHouseIOUtils(ws_id=ws_id_fitgai, \
                            lh_id=lh_id_fitgai, \
                            schema_name=schema_fitgai, \
                            table_name=source_table_name_label, \
                            relative_path=relative_path).readDF() # filter here

relative_path = f"{shared_relative_path}/{label_names[1]}" # for workload
workload_df = LakeHouseIOUtils(ws_id=ws_id_fitgai, \
                            lh_id=lh_id_fitgai, \
                            schema_name=schema_fitgai, \
                            table_name=source_table_name_label, \
                            relative_path=relative_path).readDF() # filter here

relative_path = f"{shared_relative_path}/{label_names[2]}" # for platform
platform_df = LakeHouseIOUtils(ws_id=ws_id_fitgai, \
                            lh_id=lh_id_fitgai, \
                            schema_name=schema_fitgai, \
                            table_name=source_table_name_label, \
                            relative_path=relative_path).readDF() # filter here

if iteration==0:
    relative_path = f"{shared_relative_path}/{label_names[3]}" # for support
    support_df = LakeHouseIOUtils(ws_id=ws_id_fitgai, \
                                lh_id=lh_id_fitgai, \
                                schema_name=schema_fitgai, \
                                table_name=source_table_name_label, \
                                relative_path=relative_path).readDF() # filter here

if iteration==1:
    drop_col = ["SupportLabel", "PlatformLabel", "WorkloadLabel"]
    sentiment_df = sentiment_df.drop(*drop_col)

    drop_col = ["SupportLabel", "PlatformLabel", "SentimentLabel"]
    workload_df = workload_df.drop(*drop_col)

    # TODO we keep support from one, until validation is enabled for support. then we drop
    drop_col = ["WorkloadLabel", "SentimentLabel"] 
    platform_df = platform_df.drop(*drop_col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

platform_count = platform_df.count()
workload_count = workload_df.count()
sentiment_count = sentiment_df.count()
if iteration==0:
    support_count = support_df.count()

if iteration==0:
    print(platform_count, sentiment_count, workload_count, support_count)
    if platform_count==0 or workload_count==0 or sentiment_count==0 or support_count==0:
        no_data = True

elif iteration==1:
    print(platform_count, sentiment_count, workload_count)
    if platform_count==0 or workload_count==0 or sentiment_count==0:
        no_data = True
print(no_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    sentiment_cols = set(sentiment_df.columns)
    workload_cols = set(workload_df.columns)
    platform_cols = set(platform_df.columns)
    if iteration==0:
        support_cols = set(support_df.columns)
        common_elements = list(sentiment_cols.intersection(workload_cols, platform_cols, support_cols))
        print(len(sentiment_cols),len(workload_cols),len(platform_cols),len(support_cols),len(common_elements))
        assert len(sentiment_cols)==len(workload_cols)==len(platform_cols)==len(support_cols)==len(common_elements)+1
    elif iteration==1:
        common_elements = list(sentiment_cols.intersection(workload_cols, platform_cols))
        print(len(sentiment_cols),len(workload_cols),len(platform_cols),len(common_elements))
        # TODO: below fix
        assert len(sentiment_cols)==len(workload_cols)==len(platform_cols)-1==len(common_elements)+3 # iteration and old label
        print(len(sentiment_cols),len(workload_cols),len(platform_cols),len(common_elements))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    platform_df = platform_df.orderBy(["SurveyComment", "SegmentNumber"], ascending=[True, True]).cache()
    workload_df = workload_df.orderBy(["SurveyComment", "SegmentNumber"], ascending=[True, True]).cache()
    sentiment_df = sentiment_df.orderBy(["SurveyComment", "SegmentNumber"], ascending=[True, True]).cache()
    if iteration==0:
        support_df = support_df.orderBy(["SurveyComment", "SegmentNumber"], ascending=[True, True]).cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    display(platform_df)
    display(sentiment_df)
    display(workload_df)
    if iteration==0:
        display(support_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TODO: we have droped GAI nulls from labeling, here you should look for common survey comments/segments
# this join should take care of this, but test is not done 
if not no_data:
    print(common_elements)
    dataframes = [sentiment_df, workload_df, platform_df]
    if iteration==0:
        dataframes.append(support_df)
    df_out = Postprocessing.multi_join(dataframes, on=common_elements, how="inner")
    display(df_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    df_count = df_out.count()
    if iteration==0:
        print(df_count,platform_count,sentiment_count,workload_count,support_count)
    elif iteration==1:
        print(df_count,platform_count,sentiment_count,workload_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    cols_order = [
    'SurveyComment',
    'Segment',
    'SegmentNumber',
    'Coverage',
    'CumulativeCoverage',
    'SentimentLabel',
    'WorkloadLabel',
    'SupportLabel',
    'PlatformLabel',
    'RunDateId']
    if iteration==1:
        cols_order.extend(['Platform_iteration',
        'PlatformLabel_iteration0',
        'PlatformLabel_validation',
        'Sentiment_iteration',
        'SentimentLabel_iteration0',
        'SentimentLabel_validation',
        'Workload_iteration',
        'WorkloadLabel_iteration0',
        'WorkloadLabel_validation']) 
    df_out = df_out.select(*cols_order)

    if test:
        display(df_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    workload_list = WORKLOAD_LIST.split('\n')[1:-1]
    if iteration==1:
        workload_list.append("Fabric Generic")
    # Fix label variations
    df_out = fix_label_variations(df_out, workload_list)
    print('after fix variation')
    display(df_out)

    # Rename labels
    map_workload = {}
    for _workload in workload_list:
        if "Synapse" in _workload:
            map_workload[_workload] = _workload.replace("Synapse ", "")
        elif _workload=="Not Applicable":
            map_workload[_workload] = "Fabric Generic"
        else:
            map_workload[_workload] = _workload

    df_out = rename_labels(map_workload, df_out, "WorkloadLabel")
    print('after rename labels')
    display(df_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if no_data:
    df_out = spark.createDataFrame([], Postprocessing.get_labeling_postprocessing_schema(iteration))

# write postprocessed labels to lakehouse, even if empty (report NB will expects DF to exist, even if empty)
LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, schema_fitgai, destination_table_name_label_postprocessed).writeDF(df_out, \
            partition_column=partition_column, \
            replace_column=partition_column, \
            replace_value=runDateStr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# we write back the ProcessingDate for SurveyComment that are processed
if not no_data and iteration==1:
    values_to_replace = df_out.select('SurveyComment').distinct().rdd.flatMap(list).collect()
    df_survey_overwritten = df_survey.withColumn("ProcessingDate", when(df_survey["SurveyComment"].isin(values_to_replace), runDateInt).otherwise(df_survey["ProcessingDate"]))
    df_survey_overwritten_count = df_survey_overwritten.count()
    print(df_survey_overwritten_count)
    assert df_survey_count==df_survey_overwritten_count
    LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, dim_d_r_name).writeDF(df_survey_overwritten, \
                        partition_column=None, \
                        replace_column=None)

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
    mlflow.log_metric(f'Labeling postprocessing iteration {iteration}: number of records', df_count)
    if iteration==1:
        mlflow.log_metric(f'Labeling postprocessing iteration {iteration}: number of processing dates overwritten', len(values_to_replace))
    mlflow.log_metric(f'Labeling postprocessing iteration {iteration}: Run time (sec)', time_elapsed_sec)
    mlflow.end_run()
except Exception as e:
    print('no logs collected')
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
