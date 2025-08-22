# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

OrchestrationEnvironment = "" # prod or dev
survey_type = "" #e.g. NPS
suffix = "" # e.g. prod
test = "" 
runDateStr = ""
label_runner_table_name = "" # "QC_FITGAI_Label"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OrchestrationEnvironment = "prod" # prod or dev
# survey_type = "NPS" #e.g. NPS
# suffix = "prod"
# test = "True" 
# runDateStr = "20250404"
# label_runner_table_name = "STAGE_FITGAI_Quality"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

label_runner_table_name = label_runner_table_name + "_" + suffix
destination_reporting_table_name = "FACT_FITGAI"
dim_distinct_table_name = "DIM_Distinct_Responses"

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

%run Util_HelixPyLibrary

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

ws_id_report = lakehouse_fitgai_connection[f"{OrchestrationEnvironment}_report_fitgai_ws_id"]
lh_id_report = lakehouse_fitgai_connection[f"{OrchestrationEnvironment}_report_fitgai_lh_id"]
report_schema = lakehouse_fitgai_connection['report_schema']
print('ws_id_report', ws_id_report, 'lh_id_report', lh_id_report, 'report_schema', report_schema)

fact_f_p_name = destination_reporting_table_name + "_" + suffix
dim_d_r_name = dim_distinct_table_name + "_" + suffix

_, runDateInt = produce_dates(runDateStr)
print(runDateInt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Check DIM_D_R for processing date context to check for non-schema change live-backfill
df_d_r = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, schema_fitgai, dim_d_r_name).readDF().cache()
oldest_d_r_date = df_d_r.agg({"ProcessingDate": "min"}).collect()[0][0] # Get the oldest processing date

# If the oldest processing date is today, we are beginning a live backfill
if oldest_d_r_date == runDateInt:
    backfill = True
else:
    backfill = False

if test:
    print(oldest_d_r_date, runDateInt, backfill)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Prepare latest results for scoring and insertion to tracking table
df_in = LakeHouseIOUtils(ws_id_report, lh_id_report, report_schema, fact_f_p_name).readDF()\
    .filter((col("ReportingType")==lit("Report")) & (col("SurveyType")==lit(survey_type))).cache()

if test:
    display(df_in.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pre_curr_qc_df = QualityControlChecks.gather_label_counts(df_in)

if test:
    display(pre_curr_qc_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the previous qc table if it exists
file_list = mssparkutils.fs.ls(LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, schema_fitgai).get_path())

file_exists = any(item.name == label_runner_table_name for item in file_list)
if file_exists:
    print("Appending and calcuating issues with existing records")
    # If existing QC records exist, read the table for this survey type
    # Because we focus on one SurveyType at a time, drop the SurveyType column for agnostic comparison
    prior_qc_df = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, schema_fitgai, label_runner_table_name).readDF()\
                    .filter(col("SurveyType")==lit(survey_type)).drop("SurveyType")
    # Get the latest results and ensure they're not duplicate with todays results
    latest_date = prior_qc_df.agg({"DIM_DateId": "max"}).collect()[0][0]
    if test:
        print(latest_date)
        
    latest_qc_df = prior_qc_df.filter(prior_qc_df.DIM_DateId == latest_date)
    if test:
        display(latest_qc_df)

    # Compare label counts between yesterday and today
    curr_qc_df = QualityControlChecks.compare_distinct_response_counts(latest_qc_df, pre_curr_qc_df)
    if backfill:
        curr_qc_df = curr_qc_df.withColumn("IssueFlag", lit(False))# If we are in a live backfill, force values to False
        
    if test:
        display(curr_qc_df)

    # Combine dataframes and append to QC table
    df_out = prior_qc_df.unionByName(curr_qc_df)
else:
    # If existing QC records do not exist, start a new table from scratch
    print("Starting new QC table with current records")
    df_out = pre_curr_qc_df.withColumn("IssueFlag", lit(False)) # Hardcode False because first instance

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enforce SurveyType column for attribution
df_out = df_out.withColumn("SurveyType", lit(survey_type))
if test:
    display(df_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Only allow write operation when orchestrated in prod
if OrchestrationEnvironment == "prod":
    LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, schema_fitgai, label_runner_table_name).writeDF(df_out, \
                        partition_column=None, \
                        replace_column=None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Now that the results are saved for posterity, fire IcM if any IssueFlag rows from the run date are True
issue_df = df_out.filter(col("IssueFlag")==lit(True)).filter(col("DIM_DateId")==lit(runDateInt))
if issue_df.count() > 0:
    print("Incident should be fired, collecting context...")
    label_pairs = issue_df.select("LabelType", "LabelValue").distinct().collect()

    # Restructure as Tuple for ease of use
    label_pairs = [(row.LabelType, row.LabelValue) for row in label_pairs]
    condition = (
        (col("DIM_DateId") == lit(latest_date)) & # Fetch comparison with the latest QC data
        (
            # Because we may have multiple records, attempt match of each value to guarantee coverage
            reduce(
                lambda x, y: x | y,
                [
                    (col("LabelType") == lit(LabelType)) & (F.col("LabelValue") == lit(LabelValue))
                    for LabelType, LabelValue in label_pairs
                ]
            )
        )
    )
    rows_to_append = df_out.filter(condition)
    
    # Repopulate issue_df with comparison rows
    issue_df = issue_df.unionByName(rows_to_append)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
