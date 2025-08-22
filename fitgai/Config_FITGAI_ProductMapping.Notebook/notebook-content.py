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

# this dictionary is not in used elsewhere in the code, just keeping here for the sake of explicit reference to BAP, if needed!
bap_dict = dict()
bap_dict[31] = "Power BI"
bap_dict[243] = "Fabric" # all up
bap_dict[262] = "Fabric - Real-Time Intelligence"
bap_dict[247] = "Fabric - Data Warehousing"
bap_dict[246] = "Fabric - Data Engineering"
bap_dict[245] = "Fabric - Data Science"
bap_dict[244] = "Fabric - Data Factory"
bap_dict[269] = "Fabric - Data Activator"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bap_to_Helix = dict() # key: bap, value: helix
bap_to_Helix[31] = 13
bap_to_Helix[243] = 95
bap_to_Helix[262] = 94
bap_to_Helix[247] = 92
bap_to_Helix[246] = 89
bap_to_Helix[245] = 91
bap_to_Helix[244] = 90
bap_to_Helix[269] = 97

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

helix_to_product = dict() # key: helix, value: fabric/azure
helix_to_product[13] = "Fabric"
helix_to_product[95] = "Fabric"
helix_to_product[94] = "Fabric"
helix_to_product[92] = "Fabric"
helix_to_product[89] = "Fabric"
helix_to_product[91] = "Fabric"
helix_to_product[90] = "Fabric"
helix_to_product[97] = "Fabric"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert set(bap_dict.keys())==set(bap_to_Helix.keys())
assert set(bap_to_Helix.values())==set(helix_to_product.keys())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
