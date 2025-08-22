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
suffix = "" # e.g. "prod"
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
print('ws_id_fitgai', ws_id_fitgai, 'lh_id_fitgai', lh_id_fitgai,'schema_fitgai',schema_fitgai)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

label_names = ["Sentiment", "Workload", "Platform"] #TODO improve , "support"
no_data = False

default_validation_folder = "validations" # TODO: use this in report PL and then delete"FACT_Validations"
default_validation_table = "validated" # TODO: use this in report PL and then delete "FACT_Survey_Validated"

temp_suffix = "_buffer_" + runDateStr # suffix for temporary fitgai tables
default_validation_folder += temp_suffix
default_validation_table += temp_suffix

print("default table names, validation folders: ", default_validation_folder, ", validated:", default_validation_table)


source_table_name_label = default_validation_folder +  "_" + suffix
destination_table_name_label_postprocessed = default_validation_table + "_" + suffix

print("source table: ", source_table_name_label, ", destination table:", destination_table_name_label_postprocessed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#TODO: can you change the sub-dir in labeling and use date filtering here?
shared_relative_path = f"{runDateStr[0:4]}/{runDateStr[4:6]}/{runDateStr[6:]}"

relative_path = f"{shared_relative_path}/{label_names[0]}" + "_validation" # for sentiment
sentiment_df = LakeHouseIOUtils(ws_id=ws_id_fitgai, \
                            lh_id=lh_id_fitgai, \
                            table_name=source_table_name_label, \
                            schema_name=schema_fitgai, \
                            relative_path=relative_path).readDF() # filter here

relative_path = f"{shared_relative_path}/{label_names[1]}" + "_validation" # for sentiment
workload_df = LakeHouseIOUtils(ws_id=ws_id_fitgai, \
                            lh_id=lh_id_fitgai, \
                            schema_name=schema_fitgai, \
                            table_name=source_table_name_label, \
                            relative_path=relative_path).readDF() # filter here

relative_path = f"{shared_relative_path}/{label_names[2]}" + "_validation" # for sentiment
platform_df = LakeHouseIOUtils(ws_id=ws_id_fitgai, \
                            lh_id=lh_id_fitgai, \
                            schema_name=schema_fitgai, \
                            table_name=source_table_name_label, \
                            relative_path=relative_path).readDF() # filter here

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

platform_count = platform_df.count()
workload_count = workload_df.count()
sentiment_count = sentiment_df.count()
# support_count = support_df.count()

print(platform_count, sentiment_count, workload_count) #, support_count)

if platform_count==0 or workload_count==0 or sentiment_count==0: #or support_count==0
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
    # support_cols = set(support_df.columns)

    common_elements = list(sentiment_cols.intersection(workload_cols, platform_cols)) # , support_cols
    assert len(sentiment_cols)==len(workload_cols)==len(platform_cols)==len(common_elements)+1 # ==len(support_cols)
    print(len(sentiment_cols),len(workload_cols),len(platform_cols),len(common_elements)) # ,len(support_cols)

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
    # support_df = support_df.orderBy(["SurveyComment", "Segment_Number"], ascending=[True, True]).cache()

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
    # display(support_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    print(common_elements)
    dataframes = [sentiment_df, workload_df, platform_df]#, support_df]
    df_out = Postprocessing.multi_join(dataframes, on=common_elements)
    display(df_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    df_count = df_out.count()
    print(df_count,platform_count,sentiment_count,workload_count) # ,support_count
    print(df_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    # TODO: fix this
    cols_order = [
    'SurveyComment',
    'Segment',
    'SegmentNumber',
    'Coverage',
    'CumulativeCoverage',
    'SentimentLabel',
    'SentimentLabel_validation',
    'WorkloadLabel',
    'WorkloadLabel_validation',
    'PlatformLabel',
    'PlatformLabel_validation',
    'SupportLabel',
    'RunDateId']
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
    mlflow.log_metric(f'Validation postprocessing: number of records', df_count)
    mlflow.log_metric(f'Validation postprocessing: Run time (sec)', time_elapsed_sec)
    mlflow.end_run()
except Exception as e:
    print('no logs collected')
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
