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

# MARKDOWN ********************

# # Configure lists of explanations for guided prompting

# CELL ********************

WORKLOAD_EXPLANATION_LIST = """
Power BI - Relevant to responses that discuss the visualization or interpretation of data, reporting of data or insights, business insights, coding of DAX, semantic models, Excel integration, PBI web or desktop application, pbix files, or related.
OneLake - Relevant to responses that discuss the intake or storage of data, copy or access of data from one location, data hubs, creation or use of shortcuts, or related.
Data Integration - Relevant to responses that discuss data pipelines or orchestration, data integration or movement, data ingestion/preparation/transformation, accessing multiple data sources, the use of Dataflow Gen2, use of Reflex or Trigger, or related.
Data Engineering - Relevant to responses that discuss jupytr notebooks, development IDE's or Github, spark clusters or job definitions, use of delta lake, or related.
Data Science - Relevant to responses that discuss experimentation, data enrichment or exploration, scientific modeling, model training or scoring, applications of machine learning, use of artificial intelligence, SynapseML, or related.
Data Warehousing - Relevant to responses that discuss data warehousing, cloning tables, SQL stored procedures, data schemas, use of visual query editor, database queries, distributed query processing, external data source mirroring, or related.
Real-Time Intelligence - Relevant to responses that discuss data streaming, indexing of telemetry, partitioning or formatting of data, visualizing data insights, automatically triggering actions, the use of eventhouse, Fabric Activator, real-time dashboards, the use of eventstreams, or related.
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

PLATFORM_HORIZONTAL_EXPLANATION_LIST = """
User Interface & User Experience - Relevant to responses that discuss interacting with user interface, design of user interface, navigation through the product layout, discoverability of features, or related.
Administration & Governance - Relevant to responses that discuss permissions management, sharing resources, Role Based Access Control (RBAC), data policy, or related.
Feature Gap or Feature Request - Relevant to responses that discuss missing functionality or features, requesting functionality or features, or related.
Performance - Relevant to responses that discuss the service slowness, long lasting runtimes, unexpected throughput, or related.
Pricing - Relevant to responses that discuss the cost of Fabric, billing for the service, capacity costs, licensing, price of the service as paid by the customer, or related. 
Security - Relevant to responses that discuss cybersecurity threats, software vulnerabilities, adversarial hacking, fraudulent activity, or related.
Community - Relevant to responses that discuss forums, stack overflow, sharing workspaces, social media posts, or related.
Self-Diagnostic Capability - Relevant to responses that discuss the customers' inability to determine what is wrong, failing to understand error messages, or related.
Reliability - Relevant to responses that discuss functionality not working as intended, issues related to service health, availability of services, or related.
General Feedback - Relevant to responses that do not match any of the above. There is no specific match with any other label in this list.
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SUPPORT_EXPLANATION_LIST = """
Self-Help Capability - Relevant to responses that discuss the customers' own ability to independently resolve issues, finding or accessing troubleshooting resources, debugging problems, perceived usefulness of documentation or training resources, or related.
Support Expectation - Relevant to responses that discuss software support plans, eligibility for support plans, where to get support for Fabric, communication channels for receiving assistance, or related.
Support Ticket Subject - Relevant to responses that discuss a specific support ticket, interactions with the customer support team, or related. 
Support Generic Feedback - Relevant to responses that do not match any of the above. There is no specific match with any other label in this list.
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#NOTE some dupes in artifact explanation text from Create Artifact page
ARTIFACT_EXPLANATION_LIST = """
Power BI - Report :  Create an interactive presentation of your data. 
Power BI - Paginated Report : Display tabular data in a report that's easy to print and share. 
Power BI - Scorecard : Define, track, and share key metrics for your organization. 
Power BI - Dashboard : Build a single-page data story. 
Power BI - Dataflow : Prep, clean, and transform data. 
Power BI - Datamart : Provide strategic insights from multiple sources into your business-focused or departmental data. 
Power BI - Streaming Semantic Model : Build visuals from real-time data. 
Power BI - Streaming Dataflow : Combine and transform streaming data. 
Data Activator - Reflex : Monitor datasets, queries, and event streams for patterns to trigger actions and alerts. 
Data Factory - Dataflow Gen2 : Prep, clean, and transform data. 
Data Factory - Data Pipeline : Ingest data at scale and schedule data workflows. 
Synapse Data Engineering - Lakehouse : Store big data for cleaning, querying, reporting, and sharing. 
Synapse Data Engineering - Notebook : Explore data and build machine learning solutions with Apache Spark applications. 
Synapse Data Engineering - Environment : Set up shared libraries, Spark compute settings, and resources for notebooks and Spark job definitions. 
Synapse Data Engineering - Spark Job Definition : Define, schedule, and manage your Apache Spark jobs for big data processing. 
Synapse Data Science - ML Model : Use machine learning models to predict outcomes and detect anomalies in data. 
Synapse Data Science - Experiment : Create, run, and track development of multiple models for validating hypotheses. 
Synapse Data Science - Notebook : Explore data and build machine learning solutions with Apache Spark applications. 
Synapse Data Science - Environment : Set up shared libraries, Spark compute settings, and resources for notebooks and Spark job definitions. 
Synapse Data Warehouse - Warehouse : Provide strategic insights from multiple sources into your entire business. 
Synapse Data Warehouse - Mirrored Azure SQL Database : Database connection to powerful services with modern tools to insert, query and extract data. 
Synapse Data Warehouse - Mirrored Snowflake : Mirror an existing Snowflake database 
Synapse Data Warehouse - Mirrored Azure Cosmos DB Database : Mirror an existing Azure Cosmos DB database 
Synapse Real-Time Analytics - KQL Database : Rapidly load structured, unstructured, and streaming data for querying. 
Synapse Real-Time Analytics - KQL Queryset : Run queries on your data to produce shareable tables and visuals. 
Synapse Real-Time Analytics - Real-Time Dashboard : Visualize key insights to share with your team. 
Synapse Real-Time Analytics - Eventstream : Capture, transform, and route real-time event stream to various destinations in desired format with no-code experience. 
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
