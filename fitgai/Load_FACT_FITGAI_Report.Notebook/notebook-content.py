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

# PARAMETERS CELL ********************

OrchestrationEnvironment = "" # prod or dev
survey_type = "" #e.g. NPS
suffix = "" # e.g. "prod"
test = "" # True or False, for main PL, this should be set as False
cutoff_date = ""
experiment_name = ""
runDateStr = ""
enableFuzzyDeduplication = False  # feature flag for fuzzy deduplication
intake_table_name = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OrchestrationEnvironment = "dev" # prod or dev
# survey_type = "NPS"
# suffix = "e2e"
# test = True
# cutoff_date = "2023-11-15"
# experiment_name = "fatemeh_test_schema"
# runDateStr = "20241009"  # yyyyMMdd string to be used for experiment logging and buffer table names
# enableFuzzyDeduplication = False
# intake_table_name = "FACT_Survey"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_survey_fitgai_table_name = "labeled" 
dim_distinct_table_name = "DIM_Distinct_Responses"
dim_distinct_nonresponses_table_name = "DIM_Distinct_Nonresponses"
destination_reporting_table_name = "FACT_FITGAI"
dim_duplicate_mapping_table_name = "DIM_Duplicate_Comment_Mapping" + "_" + suffix

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
fitgai_schema = lakehouse_fitgai_connection['buffer_schema']

print('ws_id_fitgai', ws_id_fitgai, 'lh_id_fitgai', lh_id_fitgai, 'fitgai_schema', fitgai_schema)

ws_id_report = lakehouse_fitgai_connection[f"{OrchestrationEnvironment}_report_fitgai_ws_id"]
lh_id_report = lakehouse_fitgai_connection[f"{OrchestrationEnvironment}_report_fitgai_lh_id"]
report_schema = lakehouse_fitgai_connection['report_schema']
print('ws_id_report', ws_id_report, 'lh_id_report', lh_id_report, 'report_schema', report_schema)

ws_id_datahub = lakehouse_fitgai_connection[f"data_hub_ws_id"]
lh_id_datahub = lakehouse_fitgai_connection[f"data_hub_lh_id"]
print('ws_id_datahub', ws_id_datahub, 'lh_id_datahub', lh_id_datahub)

fact_lh_id = lakehouse_fitgai_connection[f"src_fact_lh_id_{OrchestrationEnvironment}"]
fact_ws_id = lakehouse_fitgai_connection[f"src_fact_ws_id_{OrchestrationEnvironment}"]
fact_schema = lakehouse_fitgai_connection['src_fact_schema']
print('fact_ws_id', fact_ws_id, 'fact_lh_id', fact_lh_id, 'fact_schema', fact_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_, runDateInt = produce_dates(runDateStr) # this will be used for Experiment logging
print(runDateInt)

cutoff_date_int = produce_dates(cutoff_date)[1]
print(cutoff_date_int)

no_data = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_s_f_name = fact_survey_fitgai_table_name + "_buffer_" + runDateStr + "_" + suffix
dim_d_r_name = dim_distinct_table_name + "_" + suffix
dim_d_n_name = dim_distinct_nonresponses_table_name + "_" + suffix
fact_f_p_name = destination_reporting_table_name + "_" + suffix
print(fact_s_f_name, dim_d_r_name, dim_d_n_name, fact_f_p_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get all records from FACT_Survey
# Here in Reporting module, we need per-response identifiers
cols_FACT_Survey = ["DIM_DateId", "DIM_ProductMasterId", "FeedbackId", "SurveyComment", "Score"]

# Define filter conditions
survey_type_cond = col("SurveyType") == lit(survey_type)
date_cond = col("DIM_DateId") >= lit(cutoff_date_int)
internal_response_cond = col("InternalResponseFlag") == lit("False")

# Read data
df_fact_raw = LakeHouseIOUtils(fact_ws_id, fact_lh_id, fact_schema, intake_table_name).readDF().cache()
df = df_fact_raw.filter(survey_type_cond & internal_response_cond & date_cond)

# Select features
df_survey = df.select(*cols_FACT_Survey).cache()

# Filtering for product ids configured in PRODUCT_CONFIG
products_to_keep = list(bap_to_Helix.values())
df_survey = df_survey.filter(df_survey.DIM_ProductMasterId.isin(products_to_keep)).cache()
df_count = df_survey.count()
print('After filtering for DIM_ProductMasterId', df_count)

# Prepare for spark sql joins
df_survey.createOrReplaceTempView('vwFactSurvey')

if test:
    display(df_survey)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Isolate empty responses
df_empty = df_survey.filter((col("SurveyComment").isNull()) | (col("SurveyComment") == ""))

# df_empty ReportingType column value should be "Empty" to indicate aggregation for baseline Score
df_empty = df_empty.withColumn("ReportingType", lit("Empty"))

# Now add null values for all other columns
df_empty = df_empty.withColumn("Segment", lit(None))\
                    .withColumn("SegmentNumber", lit(None))\
                    .withColumn("Coverage", lit(None))\
                    .withColumn("CumulativeCoverage", lit(None))\
                    .withColumn("SentimentLabel", lit(None))\
                    .withColumn("WorkloadLabel", lit(None))\
                    .withColumn("SupportLabel", lit(None))\
                    .withColumn("PlatformLabel", lit(None))\
                    .withColumn("UserContext", lit(None))

if test:
    display(df_empty.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get all records from DIM_Distinct_Nonresponses
df_nonresponses = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, fitgai_schema, dim_d_n_name).readDF().cache()

if test:
    display(df_nonresponses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Combine FACT_Survey with DIM_D_N
df_survey_nonresponses = df_survey.join(df_nonresponses, ["SurveyComment"], how="inner")

# df_survey_nonresponses ReportingType column value should be "Nonresponse" to indicate special section
df_survey_nonresponses = df_survey_nonresponses.withColumn("ReportingType", lit("Nonresponse"))

# Now add null values for all other columns
df_survey_nonresponses = df_survey_nonresponses.withColumn("Segment", lit(None))\
                                                .withColumn("SegmentNumber", lit(None))\
                                                .withColumn("Coverage", lit(None))\
                                                .withColumn("CumulativeCoverage", lit(None))\
                                                .withColumn("SentimentLabel", lit(None))\
                                                .withColumn("WorkloadLabel", lit(None))\
                                                .withColumn("SupportLabel", lit(None))\
                                                .withColumn("PlatformLabel", lit(None))\
                                                .withColumn("UserContext", lit(None))
if test:
    display(df_survey_nonresponses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get all records from DIM_Distinct_Responses
cols_D_R = ["SurveyComment", "LastResponseDate", "FirstResponseDate", \
    "TokenCount", "ProcessingDate", "ReportingDate", "CharacterCount", "ResponseCount"]#TODO add hash column? do we need to read here?

df = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, fitgai_schema, dim_d_r_name).readDF()
df_known_responses = df.select(*cols_D_R).cache() # Read all of DIM_D_R so that we can overwrite later

if test:
    display(df_known_responses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Combine FACT_Survey with DIM_D_R to isolate the responses that have been processed
is_processed_cond = col("ProcessingDate").isNotNull()
df_survey_known = df_survey.join(df_known_responses.filter(is_processed_cond), ["SurveyComment"], how="inner")

if test:
    display(df_survey_known.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get buffer table with latest segmentation and labeling information
cols_S_F = ["SurveyComment", "Segment", "SegmentNumber", "Coverage", "CumulativeCoverage", \
            "SentimentLabel", "WorkloadLabel", "SupportLabel", "PlatformLabel"]

try:            
    df = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, fitgai_schema, fact_s_f_name).readDF()
    df_survey_fitgai = df.select(*cols_S_F).cache()
except Exception as e:
    print(e)
    no_data = True

if test and not no_data:
    display(df_survey_fitgai.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_data:
    # Combine the known FACT_Survey data with the latest processing results
    df_current_fitgai = df_survey_known.join(df_survey_fitgai, ["SurveyComment"], how="inner")
    # Because this is joined with the buffer table, pre-processed-repeat-responses are not included
    # Segments and labels for repeat responses not processed today need to be added at a later step

    # df_current_fitgai ReportingType column value should be "Report" to indicate core report
    df_current_fitgai = df_current_fitgai.withColumn("ReportingType", lit("Report"))\
                                        .withColumn("UserContext", lit(None))

    if test:
        display(df_current_fitgai)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cols_reporting = ["DIM_DateId", "DIM_ProductMasterId", "FeedbackId"] + cols_S_F + ["Score", "ReportingType", "UserContext"]

schema_reporting = StructType([
    StructField("DIM_DateId", IntegerType(), True), # DIM_DateId stored as int
    StructField("DIM_ProductMasterId", IntegerType(), True),
    StructField("FeedbackId", StringType(), True),
    StructField("SurveyComment", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("SegmentNumber", LongType(), True),
    StructField("Coverage", DoubleType(), True),
    StructField("CumulativeCoverage", DoubleType(), True),
    StructField("SentimentLabel", StringType(), True),
    StructField("WorkloadLabel", StringType(), True),
    StructField("SupportLabel", StringType(), True),
    StructField("PlatformLabel", StringType(), True),
    StructField("Score", IntegerType(), True),
    StructField("ReportingType", StringType(), True),
    StructField("UserContext", StringType(), True) # Keep UserContext last for easier operations downstream
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the previous reporting table for prior records if it exists
file_list = mssparkutils.fs.ls(LakeHouseIOUtils(ws_id_report, lh_id_report, report_schema).get_path())

file_exists = any(item.name == fact_f_p_name for item in file_list)
if file_exists:
    df = LakeHouseIOUtils(ws_id_report, lh_id_report, report_schema, fact_f_p_name).readDF()
    df_prior_report = df.select(*cols_reporting).cache()
    print(f"We have fetched {df_prior_report.count()} prior reported records")
else:
    print(f"Table: df_prior_report does not exist. Creating empty.")
    df_prior_report = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_reporting)

# Prepare for spark sql joins
# We will do this regardless of no_data value to ensure GDPR removal compliance
df_prior_report.createOrReplaceTempView('vwPriorReport')

if test:
    display(df_prior_report.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW 
# MAGIC vwRestatedPriorReport
# MAGIC AS
# MAGIC (
# MAGIC     SELECT FS.DIM_DateId, --Take DateId from FACT_Survey due to restatements
# MAGIC            FS.DIM_ProductMasterId,
# MAGIC            FS.FeedbackId,
# MAGIC            PR.SurveyComment,
# MAGIC            PR.Segment,
# MAGIC            PR.SegmentNumber,
# MAGIC            PR.Coverage,
# MAGIC            PR.CumulativeCoverage,
# MAGIC            PR.SentimentLabel,
# MAGIC            PR.WorkloadLabel,
# MAGIC            PR.SupportLabel,
# MAGIC            PR.PlatformLabel,
# MAGIC            FS.Score,
# MAGIC            PR.ReportingType,
# MAGIC            PR.UserContext
# MAGIC     FROM vwFactSurvey AS FS
# MAGIC     INNER JOIN vwPriorReport AS PR -- Inner join implicitly removes records dropped from FACT_Survey
# MAGIC     ON FS.FeedbackId == PR.FeedbackId
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Overwrite df_prior_report from spark sql view
df_prior_report = spark.sql("SELECT * FROM vwRestatedPriorReport")

if not no_data:
    # Enforce column order
    # This df only contains the comments that have been processed on the RunDate
    df_current_fitgai = df_current_fitgai.select(*cols_reporting)

    # If a feedbackId has been processed in the current run,
    # we will take that value over the prior report value (assuming that a new run has the better prompt + model so likely has the better label)
    df_prior_report = df_prior_report.join(
        df_current_fitgai.select('DIM_ProductMasterId', 'FeedbackId'), 
        on=['DIM_ProductMasterId', 'FeedbackId'], 
        how='left_anti'
    ).select(df_prior_report.columns)

    # Now, union with the prior report and drop duplicates to represent all processed responses historically, and all latest processed responses
    df_total_report = df_current_fitgai.union(df_prior_report).dropDuplicates()
else: # Skip join if no new segments and labels produced
    df_total_report = df_prior_report

if test:
    display(df_total_report.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if enableFuzzyDeduplication:
    # read the duplicates mapping table & isolate rows which are not yet reported
    # Check for existing duplicating mapping table


    file_list = mssparkutils.fs.ls(LakeHouseIOUtils(ws_id_report, lh_id_report, report_schema).get_path())
    duplicate_mapping_exists = any(item.name == dim_duplicate_mapping_table_name for item in file_list)

    if duplicate_mapping_exists:
        df_dup_mapping = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, fitgai_schema, dim_duplicate_mapping_table_name).readDF()
        # only read rows which are not yet added to the report
        df_new_dups = df_dup_mapping.filter(df_dup_mapping.ReportingDate.isNull()).cache()
        new_dups_count = df_new_dups.count()
    else:
        print(f"Table: {dim_duplicate_mapping_table_name} does not exist.")
        new_dups_count = 0

    print(f"We have fetched {new_dups_count} known duplicate mappings which are not yet added to fitgai report.")

    if new_dups_count != 0:
        # map the dups into fitgai report and add rows.
        joined_df = df_new_dups.alias("mapping").join(
            df_total_report.alias("fitgai"),
            col("mapping.SurveyCommentDeduped") == col("fitgai.SurveyComment"),
            "inner"
        )

        final_joined_df = joined_df.join(
            df_survey.alias("survey"),
            col("survey.SurveyComment") == col("mapping.SurveyComment"),
            "inner"
        )

        # Select the columns such that they can be added into the FITGAI report
        df_new_rows = final_joined_df.select(
            col("survey.DIM_DateId").alias("DIM_DateId"),
            col("survey.DIM_ProductMasterId").alias("DIM_ProductMasterId"),
            col("survey.FeedbackId").alias("FeedbackId"),
            col("mapping.SurveyComment").alias("SurveyComment"),
            col("fitgai.Segment").alias("Segment"),
            col("fitgai.SegmentNumber").alias("SegmentNumber"),
            col("fitgai.Coverage").alias("Coverage"),
            col("fitgai.CumulativeCoverage").alias("CumulativeCoverage"),
            col("fitgai.SentimentLabel").alias("SentimentLabel"),
            col("fitgai.WorkloadLabel").alias("WorkloadLabel"),
            col("fitgai.SupportLabel").alias("SupportLabel"),
            col("fitgai.PlatformLabel").alias("PlatformLabel"),
            col("survey.Score").alias("Score"),
            col("fitgai.ReportingType").alias("ReportingType"),
            col("fitgai.UserContext").alias("UserContext")
        )
        
        
        df_total_report = df_total_report.union(df_new_rows)  # add the duplicate rows into the report dataframe
        
        if test:
            display(df_new_rows.limit(10))
            display(df_total_report.limit(10))

        # set ReportingDate on the rows we added
        added_comments = df_new_rows.select(df_new_rows.SurveyComment.alias("AddedComment")).distinct()
        
        df_dup_mapping = df_dup_mapping.alias("mapping").join(
            added_comments,
            df_dup_mapping.SurveyComment == added_comments.AddedComment,
            "left"
        ).withColumn(
            "ReportingDate",
            when(added_comments.AddedComment.isNotNull(), lit(runDateInt)).otherwise(lit(None))
        ) 
        df_dup_mapping = df_dup_mapping.select(["SurveyComment","SurveyCommentDeduped","ReportingDate"]) # select columns from the original table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Because df_total_report contains the latest buffer and all prior buffers, new_repeat_responses are the only set of Reporting responses not included
# We use left anti join to get everything in df_survey_known that doesn't have a corresponding FeedbackId in df_total_report
# We must join exclusively on FeedbackId as DIM_DateId may have been restated
df_new_repeat = df_survey_known.join(df_total_report, ["FeedbackId"], how="left_anti")
new_repeat_count = df_new_repeat.count()

if test:
    display(df_new_repeat.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare for spark sql join because it's easier to select columns
# Here, we apply inner join to combine the pre-processed segments and labels with the new repeat responses
df_new_repeat.createOrReplaceTempView("vwNewRepeat")
df_total_report.createOrReplaceTempView("vwTotalReport")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW 
# MAGIC vwNewRepeatUpdated
# MAGIC AS
# MAGIC (
# MAGIC     WITH RankedSurveyComments AS (
# MAGIC         SELECT 
# MAGIC             SurveyComment,
# MAGIC             DIM_DateId,
# MAGIC             DIM_ProductMasterId,
# MAGIC             FeedbackId,
# MAGIC             Score,
# MAGIC             Segment,
# MAGIC             SegmentNumber,
# MAGIC             Coverage,
# MAGIC             CumulativeCoverage,
# MAGIC             SentimentLabel,
# MAGIC             WorkloadLabel,
# MAGIC             SupportLabel,
# MAGIC             PlatformLabel,
# MAGIC             ReportingType,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY SurveyComment ORDER BY DIM_DateId DESC) AS RowNum
# MAGIC         FROM vwTotalReport
# MAGIC     )
# MAGIC     SELECT DISTINCT
# MAGIC             NR.SurveyComment,
# MAGIC             NR.DIM_DateId, -- Overwrite DIM_DateId from TotalReport to propagate latest from FACT_Survey 
# MAGIC             NR.DIM_ProductMasterId,
# MAGIC             NR.FeedbackId,
# MAGIC             NR.Score,
# MAGIC             TR.Segment,
# MAGIC             TR.SegmentNumber,
# MAGIC             TR.Coverage,
# MAGIC             TR.CumulativeCoverage,
# MAGIC             TR.SentimentLabel,
# MAGIC             TR.WorkloadLabel,
# MAGIC             TR.SupportLabel,
# MAGIC             TR.PlatformLabel,
# MAGIC             TR.ReportingType -- Don't include UserContext in the joins due to mismatching FeedbackId pairings
# MAGIC     FROM vwNewRepeat AS NR
# MAGIC     INNER JOIN RankedSurveyComments AS TR
# MAGIC     ON NR.SurveyComment = TR.SurveyComment
# MAGIC     AND TR.RowNum = 1 -- Join only on the first row of each SurveyComment partition
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add the UserContext columns back for schema enforcement
df_new_repeat_updated = spark.sql("SELECT * FROM vwNewRepeatUpdated")\
                                    .withColumn("UserContext", lit(None))

if test:
    display(df_new_repeat_updated.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enforce column order
df_new_repeat_updated = df_new_repeat_updated.select(*cols_reporting)
df_survey_nonresponses = df_survey_nonresponses.select(*cols_reporting)
df_empty = df_empty.select(*cols_reporting)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Collect UserContext data from BAP IA datahub to associate response with most recently used workloads
# TODO either leave as one-off or adjust LakeHouseIOUtils to accomodate parquet read operations
usercontext_df = LakeHouseIOUtils(ws_id_datahub, lh_id_datahub, type="Files", file_relative_path="Intake/Datahub/UserContext/v1").readDF(_format="parquet").cache()

usercontext_df = usercontext_df.withColumn('UserContext', concat_ws(',', split(usercontext_df['Value'], ','))).select("FeedbackId", "UserContext")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Union all sub-df's together to contain net-new comments, and all historical nonresponses and empty responses
df_out = df_total_report.union(df_new_repeat_updated).union(df_survey_nonresponses).union(df_empty)

# Drop duplicates across all columns
# UserContext will only be populated for records in prior report, drop entire column for latest assignments
df_out = df_out.dropDuplicates().drop("UserContext")

if test:
    display(df_out.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Double check GDPR compliance of deletion requests by comparing with df_survey
# Use left-anti join to get every record in df_out that does not have a record in df_survey - these ones have been removed via destructive refresh upstream
df_removed = df_out.join(df_survey, ["FeedbackId"], how='left_anti')

if test:
    display(df_removed)

# Now use left-anti join again to select df_out records that do not match with the records we need to ensure are removed
df_out = df_out.join(df_removed, ["FeedbackId"], how='left_anti')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Combine with UserContext data once all together
df_out = df_out.join(usercontext_df, ["FeedbackId"], how='left')

if test:
    display(df_out.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enforce schema & datatype match in output table
df_out = df_out.select(*cols_reporting)

for field in schema_reporting.fields:
    df_out = df_out.withColumn(field.name, df_out[field.name].cast(field.dataType))

df_out_count = df_out.count()

if test:
    display(df_out.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare updates to DIM_Distinct_Responses
# We only update the records where we have already processed them, but have not yet reported them
update_cond = col("ReportingDate").isNull() & col("ProcessingDate").isNotNull()

df_known_responses = df_known_responses.withColumn(
    "ReportingDate",
    when(update_cond, runDateInt).otherwise(col("ReportingDate"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# map fitgai generated class names to user facing class names
# all user facing classes must map to themselves, rename_labels will set to null otherwise
product_master_map = {} # key: fitgai, value: product master (Product col)
product_master_map["Power BI"] = "Power BI"
product_master_map["OneLake"] = "OneLake"
product_master_map["Data Activator"] = "Data Activator"
product_master_map["Data Integration"] = "Data Integration"
product_master_map["Data Engineering"] = "Data Engineering"
product_master_map["Data Science"] = "Data Science"
product_master_map["Data Warehousing"] = "Data Warehousing"
# Real-Time mapped to Real<space>Time (labeling notebook creates Real-Time but the report needs Real<space>Time)
product_master_map["Real-Time Intelligence"] = "Real Time Intelligence"
product_master_map["Real Time Intelligence"] = "Real Time Intelligence"
product_master_map["Fabric Generic"] = "Fabric Generic"
df_out = df_out.withColumn("WorkloadLabel_fitgai", col("WorkloadLabel"))
df_out = rename_labels(product_master_map, df_out, "WorkloadLabel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product_master_map_id = {} # key: product master name (Product col), value: product master id
product_master_map_id["Power BI"] = "13"
product_master_map_id["OneLake"] = "86"
product_master_map_id["Data Activator"] = "97"
product_master_map_id["Data Integration"] = "90"
product_master_map_id["Data Engineering"] = "89"
product_master_map_id["Data Science"] = "91"
product_master_map_id["Data Warehousing"] = "92"
product_master_map_id["Real Time Intelligence"] = "94"
product_master_map_id["Fabric Generic"] = "95"
df_out = df_out.withColumn("Predicted_DIM_ProductMasterId", col("WorkloadLabel"))
df_out = rename_labels(product_master_map_id, df_out, "Predicted_DIM_ProductMasterId")
df_out = df_out.withColumn("Predicted_DIM_ProductMasterId", col("Predicted_DIM_ProductMasterId").cast("int"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if test:
    display(df_out.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if test:
    df_out.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('before dedupe', df_out.count())
df_out = df_out.dropDuplicates()
df_out_count_dedup = df_out.count()
print('after dedupe', df_out_count_dedup)

df_out = df_out.withColumn("SurveyType", lit(survey_type))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write combined df_out to replace FACT_FITGAI_Prod 
LakeHouseIOUtils(ws_id_report, lh_id_report, report_schema, fact_f_p_name).writeDF(df_out, \
            partition_column=None, \
            replace_column=None)

# Write updated ReportingDate values into DIM_Distinct_Responses
LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, fitgai_schema, dim_d_r_name).writeDF(df_known_responses, \
            partition_column=None, \
            replace_column=None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if enableFuzzyDeduplication:
    if new_dups_count != 0:
        # finally write out the updated mapping table 
        LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, fitgai_schema, dim_duplicate_mapping_table_name).writeDF(df_dup_mapping, \
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
    title = f'[AUTOMATED] FITGAI failed Experiment logging'
    description = f"FITGAI has failed Experiment logging for: {experiment_name} with the following exception {e}"
    severity = 4
    fields = 'Error'
    incident = IcMAutomation.createIcM(title=title, description=description, severity=severity, fields=fields, owningTeam = 'HelixData/Operations-DataScience')
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
    title = f'[AUTOMATED] FITGAI failed Experiment logging'
    description = f"FITGAI has failed Experiment logging for: {runDateStr} with the following exception {e}"
    severity = 4
    fields = 'Error'
    incident = IcMAutomation.createIcM(title=title, description=description, severity=severity, fields=fields, owningTeam = 'HelixData/Operations-DataScience')
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
    mlflow.log_metric(f'Report: number of records stored before dedupe', df_out_count)
    mlflow.log_metric(f'Report: number of records stored after dedupe', df_out_count_dedup)
    mlflow.log_metric(f'Report: number of repeated records seen before', new_repeat_count)
    mlflow.log_metric(f'Report: Run time (sec)', time_elapsed_sec)
    mlflow.end_run()
except Exception as e:
    print('no logs collected')
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
