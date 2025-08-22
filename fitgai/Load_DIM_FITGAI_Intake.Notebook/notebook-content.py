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

# PARAMETERS CELL ********************

OrchestrationEnvironment = "" # prod or dev
intake_lh_name = "" # Dataprod or AzureDataInsights
intake_table_name = "" # FACT_Survey
survey_type = "" # "NPS"
suffix = "" # e.g. "prod"
test = "" # True or False, for main PL, this should be set as False
cutoff_date = "" # 2023-11-15 is GA date
experiment_name = "" # experiment name to log metrics/artifacts/etc
runDateStr = ""  #run date in format yyyyMMdd used for Experiment logging
enableFuzzyDeduplication = False # feature flag for fuzzy deduplication

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OrchestrationEnvironment = "dev" # prod or dev
# intake_lh_name = "Dataprod"
# intake_table_name = "FACT_Survey"
# survey_type = "NPS"
# suffix = "e2e" 
# test = True # True or False, for main PL, this should be set as False
# cutoff_date = "2023-11-15" 
# experiment_name = "FITGAI_exp"
# runDateStr = "20241009" #run date in format yyyyMMdd used for Experiment logging
# enableFuzzyDeduplication = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_distinct_table_name = "DIM_Distinct_Responses"
dim_distinct_nonresponses_table_name = "DIM_Distinct_Nonresponses"
external_deps = True
no_new = False # Used to check for net new responses

dim_d_r_name = dim_distinct_table_name + "_" + suffix
dim_d_n_name = dim_distinct_nonresponses_table_name + "_" + suffix
dim_duplicate_mapping_name = "DIM_Duplicate_Comment_Mapping_" + suffix

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

fact_lh_id = lakehouse_fitgai_connection[f"src_fact_lh_id_{OrchestrationEnvironment}"]
fact_ws_id = lakehouse_fitgai_connection[f"src_fact_ws_id_{OrchestrationEnvironment}"]
fact_schema = lakehouse_fitgai_connection["src_fact_schema"]
print('fact_ws_id', fact_ws_id, 'fact_lh_id', fact_lh_id, 'fact_schema', fact_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cutoff_date_int = produce_dates(cutoff_date)[1]
print(cutoff_date_int)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get all records from FACT_Survey
cols_FACT = ["DIM_DateId", "DIM_ProductMasterId", "SurveyComment"]

# Define filter conditions
survey_type_cond = col("SurveyType") == lit(survey_type)
date_cond = col("DIM_DateId") >= lit(cutoff_date_int)
empty_cond = (col("SurveyComment").isNotNull()) & (col("SurveyComment") != "")
internal_response_cond = col("InternalResponseFlag") == lit("False")

# Read data
df_fact_raw = LakeHouseIOUtils(fact_ws_id, fact_lh_id, fact_schema, intake_table_name).readDF().cache()
df = df_fact_raw.filter(survey_type_cond & internal_response_cond & date_cond & empty_cond)

# Select features
df_survey = df.select(*cols_FACT).cache()

# Filtering for product ids configured in PRODUCT_CONFIG
products_to_keep = list(bap_to_Helix.values())
df_survey = df_survey.filter(df_survey.DIM_ProductMasterId.isin(products_to_keep)).cache()
df_count = df_survey.count()
print('After filtering for DIM_ProductMasterId', df_count)

# Drop because only needed for relevance filter
df_survey = df_survey.drop("DIM_ProductMasterId")
df_survey_count = df_survey.count()
print(df_survey_count)

df_survey_count_distinct = df_survey.select("SurveyComment").distinct().count()
print(df_survey_count_distinct)

if test:
    display(df_survey)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cols_DIM = ["SurveyComment", "LastResponseDate", "FirstResponseDate", \
    "TokenCount", "ProcessingDate", "ReportingDate", "CharacterCount", "ResponseCount"]

schema_distinct_responses = StructType([
    StructField("SurveyComment", StringType(), True),
    StructField("FirstResponseDate", IntegerType(), True), # DIM_DateId stored as int
    StructField("LastResponseDate", IntegerType(), True),
    StructField("TokenCount", LongType(), True), 
    StructField("ProcessingDate", IntegerType(), True),
    StructField("ReportingDate", IntegerType(), True),
    StructField("CharacterCount", LongType(), True),
    StructField("ResponseCount", LongType(), True) 
])

schema_nonresponses = StructType([
    StructField("SurveyComment", StringType(), True)
])

file_list = mssparkutils.fs.ls(LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, schema_fitgai).path)

# Check for known responses
dim_d_r_exists = any(item.name == dim_d_r_name for item in file_list)
if dim_d_r_exists:
    df = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, schema_fitgai, dim_d_r_name).readDF()
    df_known_responses = df.select(*cols_DIM).cache()
    df_known_responses_count = df_known_responses.count()
else:
    print(f"Table: {dim_d_r_name} does not exist, creating empty.")
    df_known_responses_count = 0
    df_known_responses = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_distinct_responses)
print(f"We have fetched {df_known_responses_count} known responses")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Now, we process new nonresponses
# Keep only records with at least one English word and atleast 2 character count
df_survey = df_survey.withColumn("CharacterCount", lit(length(col("SurveyComment"))))
contains_english_word_udf = prepare_word_filter_udf()
char_count_cond = col("CharacterCount") < 2
filter_cond = (~contains_english_word_udf(col("SurveyComment"))) | char_count_cond

df_survey_nonresponses = df_survey.filter(filter_cond)
df_survey = df_survey.filter(~filter_cond)
df_survey_count_nonresponses_removed = df_survey.count()
nonresponses_count = df_survey_nonresponses.count()
print(df_survey_count_nonresponses_removed, nonresponses_count, df_survey_count)

# Column clean up for both df's
df_survey = df_survey.drop("CharacterCount")
df_survey_nonresponses = df_survey_nonresponses.select("SurveyComment").distinct()
nonresponses_count_distinct = df_survey_nonresponses.count()
print(nonresponses_count_distinct)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if test:
    display(df_known_responses.limit(5))
    display(df_survey_nonresponses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# For GDPR compliance, remove specified responses from known responses and nonresponses if removed from FACT_Survey
# Due to upstream destructive refresh of FACT_Survey, use left-anti join to identify known responses that have been removed from FACT_Survey
df_removed_responses = df_known_responses.join(df_survey, ["SurveyComment"], how='left_anti')

# Now repeat left-anti join to remove these records from the parent dataframes
# This process only works for distinct responses and will not remove responses with at least one other occurence
df_known_responses = df_known_responses.join(df_removed_responses, ["SurveyComment"], how='left_anti')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use inner join to separate repeat responses into new DF
# This DF used to update existing records in DIM_Distinct_Responses
# Inner join propogates removed responses through to df_repeat_responses
df_repeat_responses = df_survey.join(df_known_responses, ["SurveyComment"], how="inner")

if test:
    display(df_repeat_responses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Drop columns we will need to update
# For GDPR removals with repeat responses, we re-calculate new values for count, min and max dates
df_repeat_responses = df_repeat_responses.drop("LastResponseDate").drop("FirstResponseDate").drop("ResponseCount")
if test:
    display(df_repeat_responses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Now get latest values
last_dates = df_repeat_responses.select("SurveyComment", "DIM_DateId").groupBy("SurveyComment").max().withColumnRenamed("max(DIM_DateId)", "LastResponseDate")
df_repeat_responses = df_repeat_responses.join(last_dates, on="SurveyComment", how="left")

first_dates = df_repeat_responses.select("SurveyComment", "DIM_DateId").groupBy("SurveyComment").min().withColumnRenamed("min(DIM_DateId)", "FirstResponseDate")
df_repeat_responses = df_repeat_responses.join(first_dates, on="SurveyComment", how="left")

# Drop DIM_DateId now that it's no longer needed for date checking
df_repeat_responses = df_repeat_responses.drop("DIM_DateId")

comment_counts = df_repeat_responses.select("SurveyComment").groupBy("SurveyComment").count().withColumnRenamed("count", "ResponseCount")
df_repeat_responses = df_repeat_responses.join(comment_counts, on="SurveyComment", how="left")

if test:
    display(df_repeat_responses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Duplicates were retained for ease of counting, now we remove duplicate records
df_repeat_responses = df_repeat_responses.distinct()
if test:
    display(df_repeat_responses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use left anti join to separate new responses into new DF
# This DF used to append new records to DIM_Distinct_Responses
df_new_responses = df_survey.join(df_known_responses, ["SurveyComment"], how="left_anti")
new_response_count = df_new_responses.count()

unique_new_response_count = df_new_responses.select("SurveyComment").distinct().count()
print(f"{new_response_count} new responses found.")
if new_response_count == 0:
    print("All responses already known")
    no_new = True
    df_new_responses = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_distinct_responses)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_new:
    # Create and populate columns as NULL for main pipeline to populate
    df_new_responses = df_new_responses.withColumn("TokenCount", lit(None).cast(LongType())) 
    df_new_responses = df_new_responses.withColumn("ProcessingDate", lit(None).cast(IntegerType()))

    # This column to be populated from report pipeline run
    df_new_responses = df_new_responses.withColumn("ReportingDate", lit(None).cast(IntegerType())) 

    if test:
        display(df_new_responses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_new:
    # Ensure we correctly note the first and last times we see a comment
    last_dates = df_new_responses.select("SurveyComment", "DIM_DateId").groupBy("SurveyComment").max().withColumnRenamed("max(DIM_DateId)", "LastResponseDate")
    df_new_responses = df_new_responses.join(last_dates, on="SurveyComment", how="left")

    first_dates = df_new_responses.select("SurveyComment", "DIM_DateId").groupBy("SurveyComment").min().withColumnRenamed("min(DIM_DateId)", "FirstResponseDate")
    df_new_responses = df_new_responses.join(first_dates, on="SurveyComment", how="left")

    # Drop DIM_DateId now that it's no longer needed for date checking
    df_new_responses = df_new_responses.drop("DIM_DateId")

    if test:
        display(df_new_responses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_new:    
    # Create and populate columns with per-row values
    df_new_responses = df_new_responses.withColumn("CharacterCount", lit(length(col("SurveyComment"))))

    comment_counts = df_new_responses.select("SurveyComment").groupBy("SurveyComment").count().withColumnRenamed("count", "ResponseCount")
    df_new_responses = df_new_responses.join(comment_counts, on="SurveyComment", how="left")

    if test:
        display(df_new_responses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_new:
    # Duplicates were retained for ease of counting, now we remove duplicate records
    df_new_responses = df_new_responses.distinct()

    if test:
        display(df_new_responses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Now we search for fuzzy duplicates
if enableFuzzyDeduplication:
    no_known_responses = df_known_responses.filter(col("ReportingDate").isNotNull()).count() == 0
    runDateInt = produce_dates(runDateStr)[1]
    fuzzy_duplicate_counts = 0
    if not no_new and not no_known_responses:
        # find duplicates from known responses that have already been processed and reported by fitgai
        df_new_dups_mapping = IntakeFuzzyDeduplication.make_duplicates_mapping(df_new_responses, df_known_responses.filter(col("ReportingDate").isNotNull()))

        # set ProcessingDate of df_new_responses because they do not need to be processed by the pipeline
        df_new_responses_dups_mapping = df_new_responses.join(
            df_new_dups_mapping.select("SurveyComment").withColumnRenamed("SurveyComment", "Mapping_SurveyComment"),
            df_new_responses.SurveyComment == col("Mapping_SurveyComment"),
            "left"
        )
        
        df_new_responses = df_new_responses_dups_mapping.withColumn(
            "ProcessingDate",
            when(col("Mapping_SurveyComment").isNotNull(), runDateInt).otherwise(df_new_responses.ProcessingDate)
        ).select(*cols_DIM)

        fuzzy_duplicate_counts = df_new_dups_mapping.count()

        if test:
            print(f"found {fuzzy_duplicate_counts} fuzzy duplicates")
            display(df_new_dups_mapping.limit(5))
            display(df_new_responses.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if enableFuzzyDeduplication:
    if not no_new and not no_known_responses:
        # update existing dups_mapping table
        file_list = mssparkutils.fs.ls(LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai, schema_fitgai).path)

        dim_duplicate_mapping_exists = any(item.name == dim_duplicate_mapping_name for item in file_list)
        if dim_duplicate_mapping_exists:
            df = LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, dim_duplicate_mapping_name).readDF()
            df_dups_mapping = df.cache()
            df_known_dups_count = df_dups_mapping.count()
            
            if test:
                print(f"We have fetched {df_known_dups_count} known responses")
                display(df_dups_mapping)
            df_dups_mapping = df_dups_mapping.union(df_new_dups_mapping)

        else:
            print(f"Table: {dim_duplicate_mapping_name} does not exist, creating new.")
            df_dups_mapping = df_new_dups_mapping
        
        # save the updates mapping table
        LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai,schema_fitgai, dim_duplicate_mapping_name).writeDF(df_dups_mapping, \
                partition_column=None, \
                replace_column=None)
        if test:
            display(df_dups_mapping.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enforce column order
df_repeat_responses = df_repeat_responses.select(*cols_DIM)
df_new_responses = df_new_responses.select(*cols_DIM)

# Union together
df_out = df_repeat_responses.union(df_new_responses)

# Enforce datatype match
for field in schema_distinct_responses.fields:
    df_out = df_out.withColumn(field.name, df_out[field.name].cast(field.dataType))

df_out_count = df_out.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if test:
    print(df_out_count)
    display(df_out.sort("ProcessingDate", ascending=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write combined df_out to replace DIM_Distinct_Responses
LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, dim_d_r_name).writeDF(df_out, \
            partition_column=None, \
            replace_column=None)

# Write df_survey_nonresponses
LakeHouseIOUtils(ws_id_fitgai, lh_id_fitgai,schema_fitgai, dim_d_n_name).writeDF(df_survey_nonresponses, \
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
    experiment = mlflow.set_experiment(experiment_name)
    run = mlflow.start_run(run_name=runDateStr)

    mlflow.log_metric('Intake: Total responses in source', df_survey_count)
    mlflow.log_metric('Intake: Total unique responses in source', df_survey_count_distinct)
    mlflow.log_metric('Intake: Total known responses fetched from before', df_known_responses_count)
    mlflow.log_metric('Intake: Total responses in source, exclduing nonresponses', df_survey_count_nonresponses_removed)
    mlflow.log_metric('Intake: Total nonresponses', nonresponses_count)
    mlflow.log_metric('Intake: Total unique nonresponses', nonresponses_count_distinct)
    mlflow.log_metric('Intake: Total new responses found', new_response_count)
    mlflow.log_metric('Intake: Total unique new responses found', unique_new_response_count)
    mlflow.log_metric('Intake: Total responses stored in DIM distinct table', df_out_count)
    if enableFuzzyDeduplication:
        mlflow.log_metric('Intake: Total new fuzzy duplicates found in new responses', fuzzy_duplicate_counts)
    mlflow.log_param("Survey Type", survey_type)
    mlflow.log_metric('Intake: Run time (sec)', time_elapsed_sec)

    mlflow.end_run()
except Exception as e:
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
