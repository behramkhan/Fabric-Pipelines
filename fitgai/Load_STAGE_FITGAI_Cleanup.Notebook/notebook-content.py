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

OrchestrationEnvironment = "" # "prod"
partition_column = "" # "RunDateId"
suffix = "" # "prod"
test = "" # True # True or False, for main PL, this should be set as False
retention_days = "" # 3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = "Curate"
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
print('ws_id_fitgai: ', ws_id_fitgai, ', lh_id_fitgai:', lh_id_fitgai)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exempt_date_cutoff = datetime.today() - timedelta(days=retention_days)
path = f"abfss://{ws_id_fitgai}@msit-onelake.dfs.fabric.microsoft.com/{lh_id_fitgai}/Tables/{schema}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

staged = get_staged_tables(path, exempt_date_cutoff)
len(staged)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

no_buffers = False
if len(staged)==0:
    no_buffers = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not no_buffers:
    # Finally, delete all temporary _buffer_ tables from FITGAI LH
    for table in staged:
        path_to_delete = f"{path}/{table}"
        print('deleting: ', path_to_delete)
        mssparkutils.fs.rm(path_to_delete, True)
        print('deleted: ', path_to_delete)
        print(50*'*')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

end_time = time.time()
time_elapsed_sec = end_time - start_time # sec
print(time_elapsed_sec/60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
