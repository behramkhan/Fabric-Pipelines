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

%run /Config_FITGAI_ProductMapping

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run /Config_FITGAI_SystemPrompts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run /Config_FITGAI_PromptOutputsList

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run /Config_FITGAI_LabelExplanation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_fitgai_connection = {
'report_schema': 'Dataprod',
'src_fact_schema': 'Dataprod',
'buffer_schema': 'Curate',

'prod_report_fitgai_lh_id': '31dd665e-95f3-4575-9f46-70ea5903d89b', 
'prod_report_fitgai_ws_id': '2d2e0ae2-9505-4f0c-ab42-e76cc11fb07d', 

'prod_buffer_fitgai_lh_id': '31dd665e-95f3-4575-9f46-70ea5903d89b', 
'prod_buffer_fitgai_ws_id': '2d2e0ae2-9505-4f0c-ab42-e76cc11fb07d', 

'dev_report_fitgai_lh_id': '0a5db5a9-43d9-4c2f-8029-b1ad24347cce', 
'dev_report_fitgai_ws_id': 'c6b62e39-824b-4614-9823-c451512cc8d4', 

'dev_buffer_fitgai_lh_id': '0a5db5a9-43d9-4c2f-8029-b1ad24347cce',
'dev_buffer_fitgai_ws_id': 'c6b62e39-824b-4614-9823-c451512cc8d4',

# These are the guids for evaluation, the lakehouses will contain only the data we hand label.
# Not for use outside evaluation 😊
'evaluation_report_fitgai_lh_id': '38e8f882-1aa5-4dbe-8aa3-f0db53ac62bd', 
'evaluation_report_fitgai_ws_id': '006a9817-c85d-428d-b158-1de086a06c5e',   

'evaluation_buffer_fitgai_lh_id': '38e8f882-1aa5-4dbe-8aa3-f0db53ac62bd',  
'evaluation_buffer_fitgai_ws_id': '006a9817-c85d-428d-b158-1de086a06c5e',

'src_fact_lh_id_evaluation': "38e8f882-1aa5-4dbe-8aa3-f0db53ac62bd",
'src_fact_ws_id_evaluation': "006a9817-c85d-428d-b158-1de086a06c5e", 

# for raw data, each entry points to the FACT_Survey table! 
'src_fact_lh_id_dev': "0a5db5a9-43d9-4c2f-8029-b1ad24347cce",
'src_fact_ws_id_dev': "c6b62e39-824b-4614-9823-c451512cc8d4", 

'src_fact_lh_id_test': "c96f2b5c-4237-4eea-8dac-51027edd3088",
'src_fact_ws_id_test': "6eaa11fc-511c-40d1-b090-72a27764fd75", 

'src_fact_lh_id_prod': "31dd665e-95f3-4575-9f46-70ea5903d89b",
'src_fact_ws_id_prod': "2d2e0ae2-9505-4f0c-ab42-e76cc11fb07d", 

# for user context
'data_hub_lh_id': "31dd665e-95f3-4575-9f46-70ea5903d89b", 
'data_hub_ws_id': "2d2e0ae2-9505-4f0c-ab42-e76cc11fb07d"
}



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

segmentation_caching_config = {
    "dev": {
        "enabled": True,
        "lh_id": lakehouse_fitgai_connection['dev_buffer_fitgai_lh_id'],
        "ws_id": lakehouse_fitgai_connection['dev_buffer_fitgai_ws_id'],
        "schema": lakehouse_fitgai_connection['buffer_schema']
    },
    "test": {
        "enabled": False
    },
    "evaluation": {
        "enabled": True,
        "lh_id": lakehouse_fitgai_connection['evaluation_buffer_fitgai_lh_id'],
        "ws_id": lakehouse_fitgai_connection['evaluation_buffer_fitgai_ws_id'],
        "schema": lakehouse_fitgai_connection['buffer_schema']
    },
    "prod": {
        "enabled": False
    }
}



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Dependencies

# CELL ********************

import re
import pytz
import nltk
import mlflow
import warnings
import synapse.ml.core

from typing import Union
from pyspark.sql import Row
from itertools import chain
from pyspark.sql import Window
from pyspark.sql.types import *
from functools import reduce, partial
from pyspark.sql.dataframe import DataFrame
from datetime import datetime, date, timedelta
from synapse.ml.services.openai import OpenAIChatCompletion
from pyspark.sql.functions import lit, col, explode, length, when, split, struct, size, sum, udf, max, first, last, count, coalesce
from pyspark.sql.functions import arrays_zip, to_date, create_map, regexp_replace, array_distinct, array_except, concat_ws, countDistinct

from sklearn.neighbors import NearestNeighbors

mlflow.autolog(disable=True)

if external_deps:
    import Levenshtein as lev
    from fuzzywuzzy import process
    try:
        if enableFuzzyDeduplication:
            from sentence_transformers import SentenceTransformer
    except NameError:
        # the enableFuzzyDeduplication flag variable does not exist,
        # assuming that means the feature is disabled
        enableFuzzyDeduplication = False

warnings.filterwarnings("ignore")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Define utility functions and classes

# CELL ********************

class LakeHouseIOUtils():
    """
    A utility class for reading and writing data to a Delta Lakehouse in Azure Blob Storage.
    """
    def __init__(self, ws_id="", lh_id="",schema_name="", table_name="", relative_path=None, file_relative_path=None, type="Tables"):
        """
        Initializes a new instance of the LakeHouseIOUtils class.

        Args:
        - ws_id (str): The ID of the workspace containing the Lakehouse.
        - lh_id (str): The ID of the Lakehouse.
        - schema_name: The schema under which the table is stored.
        - table_name (str): The name of the table to read from or write to.
        - relative_path (str): The relative path within the Delta Lakehouse to read from or write to.
        """
        self.ws_id = ws_id
        self.lh_id = lh_id
        self.table_name = table_name
        self.schema_name = schema_name
        self.relative_path = relative_path
        self.file_relative_path = file_relative_path
        self.type = type
        self.path = self.get_path()

    def get_path(self):
        assert self.type in ["Tables", "Files"]
        # Construct the path using the workspace ID, lakehouse ID, and table name
        if self.type=="Tables":
            path = f"abfss://{self.ws_id}@msit-onelake.pbidedicated.windows.net/{self.lh_id}/Tables/{self.schema_name}/{self.table_name}"
        elif self.type=="Files":
            path = f"abfss://{self.ws_id}@msit-onelake.pbidedicated.windows.net/{self.lh_id}/Files/{self.file_relative_path}"

        if self.relative_path is not None:
            path = f"{path}/{relative_path}"
        print(path)
        return path

    def readDF(self, _format="delta"):
        """
        load a dataframe from lakehouse path
        """
        return spark.read.format(_format).load(self.path).cache()

    def writeDF(self, spark_df: DataFrame, partition_column : str =None, replace_column : str = None, replace_value=None):   
        writer = spark_df.write.format("delta").mode("overwrite")
        
        if replace_column is not None: # TODO: this is not tested, do NOT use
            assert replace_value is not None
            # If a replace column and value are provided, set the "replaceWhere" option for the writer
            writer = writer.option("replaceWhere", f"{replace_column} == {replace_value}")
        
        if partition_column is not None:
            # If a partition column is provided, set the "partitionBy" option for the writer
            writer = writer.partitionBy(*[partition_column])

        return writer.save(self.path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class GPTPreprocessing():
    """
    this is a collection of preprocessing utilities to prepare data for GPT based operations 
    """
    def __init__(self):
        pass
    
    @staticmethod
    def make_message(role: str, content: str):
        return Row(role=role, content=content, name=role)

    @staticmethod
    def create_user_prompt(parent_text, child_text, label_list, label_explain_list=None, label_name="", iteration=0, justification=""):
        # iteration = 1
        
        if iteration==1 and len(justification)>1:
            justification_prompt = f"""
                            Before you provide your answer, interpret the following analysis related to your task:
                            {justification}
                            You must interpret that perspective before answering with your chosen label.
                            """
        else:
            justification_prompt = ""

        if label_name=="Workload":
            assert label_explain_list is not None
            label_name = label_name.capitalize() # Proper nouns require capitalization
            user_message_prompt = f"""
                Please read the following list of {label_name}s and their explanations to understand them: {label_explain_list}
                The following list of {label_name}s are the only possible answers: {label_list}
                Read the targeted segment of this response.
                Segment: {child_text}        
                Read the full survey response and determine whether there are any references outside of that segment related to your answer.
                Survey response: {parent_text}
                {justification_prompt}
                """
        
        elif label_name=="Artifact": #TODO DOES NOT EXIST YET
            assert label_explain_list is not None
            label_name = label_name.capitalize() # Proper nouns require capitalization
            user_message_prompt = f"""
                Please read the following list of {label_name}s and their explanations to understand them: {label_explain_list}
                The following list of {label_name}s are the only possible answers: {label_list}
                Read the targeted segment of this response.
                Segment: {child_text}        
                Read the full survey response and determine whether there are any references outside of that segment related to your answer.
                Survey response: {parent_text}
                {justification_prompt}
                """
        
        elif label_name=="Sentiment": # Exclusively consider the segment independent of parent_text
            user_message_prompt = f"""
                The following list of labels are the only possible answers: {label_list}
                Now read the following segment of a survey response and reply with your chosen label that best represents sentiment, connotation, and implication.
                Segment: {child_text}
                {justification_prompt}
                """
        
        elif label_name=="Platform":
            assert label_explain_list is not None
            user_message_prompt = f"""
                Please read the following list of label types and their explanations to understand them: {label_explain_list}
                The following list of labels are the only possible answers: {label_list}
                Read the targeted segment of this response.
                Segment: {child_text}        
                Finally, read the full survey response and determine whether there are any references outside of that segment related to your answer.
                Survey response: {parent_text}
                {justification_prompt}
                """

        elif label_name=="Support": # TODO No justification insertion until accuracy feedback is collected 
            assert label_explain_list is not None
            user_message_prompt = f"""
                Please read the following list of {label_name} labels and their explanations to understand them: {label_explain_list}
                The following list of {label_name} labels are the only possible answers: {label_list}
                Read the targeted segment of this response.
                Segment: {child_text} 
                Finally, read the full survey response and determine whether there are any references outside of that segment related to your answer.
                Survey response: {parent_text}
                """
        else:
            print(f"{label_name} is not an acceptable label! This will raise an error!")

        return str(user_message_prompt)

    def create_validation_user_prompt(parent_text, child_text, original_label, label_explain_list="", label_name=""):
        # assert label_name in ["sentiment", "workload", "artifact", "platform"]
        if label_name=="Workload": 
            assert label_explain_list is not None
            label_name = label_name.capitalize() # Proper nouns require capitalization
            user_message_prompt = f"""
                Please read the following list of labels and their explanations to understand them: {label_explain_list}
                Now read the entire survey response.
                Survey Response: {parent_text}
                Now read the target segment of that response.
                Segment: {child_text}
                This segment has been labeled as the following Workload: {original_label}
                Now answer with **Agree** or **Disagree** to indicate your opinion of the label.
                """
        
        elif label_name=="Artifact": #TODO DOES NOT EXIST YET
            assert label_explain_list is not None
            label_name = label_name.capitalize() # Proper nouns require capitalization
            user_message_prompt = f"""
                Please read the following list of labels and their explanations to understand them: {label_explain_list}
                Now read the entire survey response.
                Survey Response: {parent_text}
                Now read the target segment of that response.
                Segment: {child_text}
                This segment has been labeled as the following Artifact: {original_label}
                Now answer with **Agree** or **Disagree** to indicate your opinion of the label.
                """
        
        elif label_name=="Sentiment": # Exclusively consider the segment independent of parent_text
            user_message_prompt = f"""
                Please read the following segment of a survey response.
                Segment: {child_text}
                This segment has been assigned the following sentiment: {original_label}
                Now answer with **Agree** or **Disagree** to indicate your opinion of the label.
                """

        elif label_name=="Platform":
            user_message_prompt = f"""
                Please read the following list of labels and their explanations to understand them: {label_explain_list}
                Now read the entire survey response.
                Survey Response: {parent_text}
                Now read the target segment of that response.
                Segment: {child_text}
                This segment has been assigned the following label: {original_label}
                Now answer with **Agree** or **Disagree** to indicate your opinion of the label.
                """
        else:
            print(f"{label_name} is not an acceptable label! This will raise an error!")

        return str(user_message_prompt)


    def add_system_user(row):
        """ 
        row based operation, use this function for mapping on spark dataframe
        here, row only has one column, and the col is "SurveyComment"
        """
        return (row, GPTPreprocessing.make_message("system", system_message_prompt), GPTPreprocessing.make_message("user", str(row)))

    def combine_system_user(row):
        """ 
        row based operation, use this function for mapping on spark dataframe

        these are the cols in the df that inputs this function:

        ["SurveyComment", "modified_SurveyComment_user", "modified_SurveyComment_system"]

        original survey column, user message, system message, combined system-user message

        """

        res_return = (row.SurveyComment, \
                      row.modified_SurveyComment_user, \
                      row.modified_SurveyComment_system, \
                      list([row.modified_SurveyComment_user, row.modified_SurveyComment_system])) 

        return res_return

    def add_system_user_label(row):
        # TODO: (for later) we can combine this with add_system_user function for segmentation
        # iteration = 1
        """ 
        row based operation, use this function for mapping on spark dataframe

        row has two elements, parent text,SurveyComment [0] & child text,Segment [1]
        """

        sys_msg = GPTPreprocessing.make_message("system", system_message_prompt_label)
        if iteration==0:
            user_msg_created = GPTPreprocessing.create_user_prompt(row.SurveyComment, row.Segment, label_list, label_explain_list, label_name)
        elif iteration==1:
            user_msg_created = GPTPreprocessing.create_user_prompt(row.SurveyComment, row.Segment, label_list, label_explain_list, label_name, iteration, row.temp_validation.Justification)

        user_msg = GPTPreprocessing.make_message("user", user_msg_created)
        
        if iteration==0:
            _ret = (row.SurveyComment, row.Segment, sys_msg, user_msg)
        elif iteration==1:
             _ret = (row.SurveyComment, row.Segment, row.temp_validation, sys_msg, user_msg)

        return _ret

    def add_system_user_label_validation(row):
        """ 
        row based operation, use this function for mapping on spark dataframe

        row has three elements, SurveyComment [0], Segment [1], label_to_validate[2]
        """

        row_dict = row.asDict()

        sys_msg = GPTPreprocessing.make_message("system", system_message_prompt_validation)
        user_msg_created = GPTPreprocessing.create_validation_user_prompt(row.SurveyComment, row.Segment, row_dict[original_label_col], label_explain_list, label_name)
        user_msg = GPTPreprocessing.make_message("user", user_msg_created)
        
        return (row.SurveyComment, row.Segment, row_dict[original_label_col], sys_msg, user_msg)

    def combine_system_user_label(row):
        # TODO: (for later) we can combine this with combine_system_user function for segmentation
        # iteration = 1
        """ 
        row based operation, use this function for mapping on spark dataframe

        ["SurveyComment", "Segment",  "system", "user"]
        """
        if iteration==0:
            _ret = (row.SurveyComment, row.Segment, list([row.system, row.user])) # return is original res, sub verbatim, combined system-user message
        elif iteration==1:
            _ret = (row.SurveyComment, row.Segment, row.temp_validation, list([row.system, row.user])) # return is original res, sub verbatim, combined system-user message

        return _ret

    def combine_system_user_label_validation(row):
        # TODO: (for later) we can combine this with combine_system_user function for segmentation
        """ 
        row based operation, use this function for mapping on spark dataframe

        ["SurveyComment", "Segment", "Label",  "system", "user"]
        """
        row_dict = row.asDict()
        return (row.SurveyComment, row.Segment,  row_dict[original_label_col], list([row.system, row.user])) # return is original res, sub verbatim, combined system-user message

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class Preprocessing():
    """
    this is a collection of preprocessing utilities for segmentation
    """
    def __init__(self):
        pass
    
    @staticmethod
    def segmentation(df: DataFrame):
        # Map the add_system_user function to each row in the RDD
        new_rdd = df.rdd.map(GPTPreprocessing.add_system_user) 
        # Convert the RDD back to a DataFrame with specified column names
        new_df = new_rdd.toDF(["SurveyComment", "modified_SurveyComment_user", "modified_SurveyComment_system"])

        # Map the combine_system_user function to each row in the RDD
        new_rdd = new_df.rdd.map(GPTPreprocessing.combine_system_user) 
        # Convert the RDD back to a DataFrame with specified column names
        new_df = new_rdd.toDF(["SurveyComment", "modified_SurveyComment_user",  "modified_SurveyComment_system", "message"])

        # Select specific columns from the DataFrame and return it, caching it for future use
        gpt_df = new_df.select("SurveyComment.SurveyComment", "message")
        return gpt_df.cache()

    @staticmethod
    def labeling(df: DataFrame, label_list: list, label_explain_list: list, label_name: str, system_message_prompt_label : str, iteration : int):

        print(f"The label name is: {label_name} \n\n , \
              The lable list is: {label_list} \n\n , \
              The label explaination list is: {label_explain_list} \n\n , \
              The system prompt for this label is: {system_message_prompt_label}")
        print("iteration: ", iteration)
        
        # Map the add_system_user function to each row in the RDD
        new_rdd = df.rdd.map(GPTPreprocessing.add_system_user_label) 
       
        # Convert the RDD back to a DataFrame with specified column names

        if iteration==0:
            schema = ["SurveyComment", "Segment",  "system", "user"]
        elif iteration==1:
            schema = ["SurveyComment", "Segment",  "temp_validation", "system", "user"]
        
        new_df = new_rdd.toDF(schema)

        # Map the combine_system_user function to each row in the RDD
        new_rdd = new_df.rdd.map(GPTPreprocessing.combine_system_user_label) 

        if iteration==0:
            schema = ["SurveyComment", "Segment",  "message"]
        elif iteration==1:
            schema = ["SurveyComment", "Segment",  "temp_validation", "message"]
        
        # Convert the RDD back to a DataFrame with specified column names
        gpt_df = new_rdd.toDF(schema)

        return gpt_df.cache()

    @staticmethod
    def validation(df: DataFrame, original_label_col: str, label_name: str, system_message_prompt_validation : str):
        # Map the add_system_user function to each row in the RDD
        new_rdd = df.rdd.map(GPTPreprocessing.add_system_user_label_validation) 
        
        # Convert the RDD back to a DataFrame with specified column names
        new_df = new_rdd.toDF(["SurveyComment", "Segment", original_label_col,  "system", "user"])

        # Map the combine_system_user function to each row in the RDD
        new_rdd = new_df.rdd.map(GPTPreprocessing.combine_system_user_label_validation) 
        # Convert the RDD back to a DataFrame with specified column names
        gpt_df = new_rdd.toDF(["SurveyComment", "Segment", original_label_col, "message"])

        return gpt_df.cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class Postprocessing():
    """
    this is a collection of postprocessing utilities.
    The methods of this calss are PySpark UDFs and methods which call UDFs.
    You cannot refer the spark context from within this class!

    """
    def __init__(self):
        pass

    @staticmethod
    def labeling_postprocess_set_format_single_label(row): 
        """
        # for labeling postprocessing to just set the format of the output as a string
        """
        _res = (row.SurveyComment, row.Segment, \
                str(row.content[0]))
        return _res

    @staticmethod
    def labeling_single_label(df: DataFrame):

        new_rdd = df.rdd.map(Postprocessing.labeling_postprocess_set_format_single_label)

        schema = StructType([
        StructField("SurveyComment", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("content", StringType(), True),
        ])
        df_out = new_rdd.toDF(schema)
        return df_out.cache()
        
    @staticmethod
    def clean_list_elements(_res : str):
        _res_clean = _res.strip().strip('-').strip('"')
        return _res_clean

    @staticmethod
    def clean_gpt_response(result : list):

        result = result[0] # result from GPT is a list by default, we are assuming one element per row
        result_list = result.splitlines()

        segmentList = list(map(Postprocessing.clean_list_elements, result_list))

        # Line below is to address the duplicate issue, here is an example that generated a bug downstream
        # response: Impressive Product, but hard to learn :)
        # and GPT returned: ["---Impressive Product---\n\"Impressive Product\""]

        def remove_duplicates(input_list):
            return list(dict.fromkeys(input_list))
        
        segmentList = remove_duplicates(segmentList) # to remove duplicate values from the segmentation steps

        # to remove empty segments coming from GPT, before assigning counter
        def remove_empty_strings(input_list):
            return [string for string in input_list if string]
        
        segmentList = remove_empty_strings(segmentList)

        subcount = list(range(0, len(segmentList)))

        return segmentList, subcount

    @staticmethod
    def run_clean_gpt_response_on_df(row): 
        """ 
        row based operation, use this function for mapping on spark dataframe
        """
        func_ = Postprocessing.clean_gpt_response(row.content)

        result_return = (row.SurveyComment, func_[0], func_[1])
        return result_return
 
    @staticmethod
    def segmentation(df: DataFrame):
        func_start_timer = time.time()
        
        # Apply 'run_clean_gpt_response_on_df' function to each row in the RDD
        DF_rdd = df.rdd.map(Postprocessing.run_clean_gpt_response_on_df)  

        _cols = ["SurveyComment", "Segment", "SegmentNumber"]

        # Convert RDD to DataFrame with defined column names
        df_cleaned = DF_rdd.toDF(_cols)

        # Combine 'Segment' and 'SegmentNumber' columns into a single column 'combined'
        df_cleaned = df_cleaned.withColumn("combined", arrays_zip(df_cleaned.Segment, df_cleaned.SegmentNumber))
        
        # Explode the 'combined' column to create new rows for each combination of 'Segment' and 'SegmentNumber'
        df_cleaned = df_cleaned.withColumn("exploded", explode(df_cleaned.combined))

        post_processed = df_cleaned.select(df_cleaned.SurveyComment, df_cleaned.exploded.Segment, \
                                        df_cleaned.exploded.SegmentNumber)

        post_processed = post_processed.withColumnRenamed("exploded.Segment", "Segment")
        post_processed = post_processed.withColumnRenamed("exploded.SegmentNumber", "SegmentNumber")
        
        return post_processed.cache()  

    # Use to join two dataframes together 
    @staticmethod
    def join(df1: DataFrame, df2: DataFrame, on: Union[str, list], how : str="inner", drop: bool=True):
        if isinstance(on, str):
            df_out = df1.join(df2, df1[on]==df2[on], how=how)
            if drop:
                df_out = df_out.drop(df2[on])
        if isinstance(on, list):
            df_out = df1.join(df2, on, how=how)
             
        return df_out.cache()

    # Use to join a list of dataframes together
    @staticmethod
    def multi_join(df_list: list, on:Union[str,list], how : str="inner", drop:bool=True):
        return(reduce(partial(Postprocessing.join, on=on, how=how), df_list))
    
    @staticmethod
    def get_labeling_postprocessing_schema(iteration):
        base_fields = [
            StructField('SurveyComment', StringType(), True),
            StructField('Segment', StringType(), True),
            StructField('SegmentNumber', LongType(), True),
            StructField('Coverage', DoubleType(), True),
            StructField('CumulativeCoverage', DoubleType(), True),
            StructField('SentimentLabel', StringType(), True),
            StructField('WorkloadLabel', StringType(), True),
            StructField('SupportLabel', StringType(), True),
            StructField('PlatformLabel', StringType(), True),
            StructField('RunDateId', StringType(), True)  # assuming it's a YYYYMMDD int
        ]

        # Add extra fields for iteration == 1
        if iteration == 1:
            extra_fields = [
                StructField('Platform_iteration', IntegerType(), True),
                StructField('PlatformLabel_iteration0', StringType(), True),
                StructField('PlatformLabel_validation', StringType(), True),
                StructField('Sentiment_iteration', StringType(), True),
                StructField('SentimentLabel_iteration0', StringType(), True),
                StructField('SentimentLabel_validation', StringType(), True),
                StructField('Workload_iteration', StringType(), True),
                StructField('WorkloadLabel_iteration0', StringType(), True),
                StructField('WorkloadLabel_validation', StringType(), True)
            ]
            schema_fields = base_fields + extra_fields
        else:
            schema_fields = base_fields

        # Build schema and create empty DataFrame
        schema = StructType(schema_fields)
        return schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

regex_sanitize = r'[!?\-]+'  # Remove all undesirable characters
regex_split = r'[ ,.;:"\']+'  # Split on common separators

# Function to sanitize and split strings into word sets
def sanitize_split_string(string):
    sanitized = regexp_replace(string, regex_sanitize, '')  # Sanitize
    words = split(sanitized, regex_split)  # Split
    return array_distinct(words)  # Remove duplicates

def validate_gpt_output(gpt_df, runDateInt, partition_column):
    # Assert input is a Spark DataFrame
    assert isinstance(gpt_df, DataFrame)

    # Apply the sanitize and split function to create word sets for SurveyComment and Segment
    gpt_df = gpt_df.withColumn('super_set', sanitize_split_string(col('SurveyComment'))) \
                   .withColumn('sub_set', sanitize_split_string(col('Segment')))

    # Calculate the difference set and coverage
    gpt_df = gpt_df.withColumn('diff_set', array_except(col('super_set'), col('sub_set'))) \
                   .withColumn('coverage_perc', 1 - (size(col('diff_set')) / size(col('super_set'))))

    # Window specification to calculate cumulative coverage
    window_spec = Window.partitionBy("SurveyComment").orderBy("SegmentNumber")

    # Calculate cumulative coverage
    gpt_df = gpt_df.withColumn('CumulativeCoverage', sum('coverage_perc').over(window_spec))

    # Add date values
    gpt_df = gpt_df.withColumn(partition_column, lit(runDateInt))

    # Select the final desired columns and cache the DataFrame
    gpt_df = gpt_df.select("SurveyComment", "Segment", "SegmentNumber", partition_column,
                           "coverage_perc", "CumulativeCoverage").withColumnRenamed("coverage_perc", "Coverage")

    return gpt_df.cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def produce_dates(target_date:str=None, tz:str=None):
    
    if target_date: # Attempt conversion of string
        candidate_date_fmts = ['%Y%m%d', '%Y-%m-%d']  
        date_fmt = None
        for candidate_date_fmt in candidate_date_fmts:
            try:
                parsed_date = datetime.strptime(target_date, candidate_date_fmt)
                date_fmt = candidate_date_fmt
                break
            except ValueError:
                pass
        if date_fmt:
            target_date = parsed_date.date()
        else:
            raise ValueError(f"target_date does not conform to any formats {candidate_date_fmts}")
    else:
        # Assume today  if not passed
        if tz=="PST": # in PST
            pst_tz = pytz.timezone('US/Pacific')
            target_date = datetime.now(pytz.utc).astimezone(pst_tz).date()
        else: #assume UTC
            target_date = datetime.today().date() # Assume today if not passed
        date_fmt = '%Y-%m-%d'
    
    dateStr = target_date.strftime(date_fmt).replace('-', '')
    dateInt = int(dateStr)
    return dateStr, dateInt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_date(s):
    # Compile the regular expression pattern for 8 digits
    pattern = re.compile(r'\b\d{8}\b')
    
    # Search for the pattern in the string
    
    parts = s.split('_')
    for _p in parts:
        match = pattern.search(_p)
        if match:
            return match.group()

def get_staged_tables(p, exempt_date_cutoff):
    print(p)
    file_list = mssparkutils.fs.ls(p)
    list_of_buffers = [file.name for file in file_list if ("_buffer_" in file.name)]
    not_staged = list()
    staged = list()
    for _buffer in list_of_buffers:
        _date = extract_date(_buffer)
        if _date is not None:
            date_object = datetime.strptime(_date, "%Y%m%d")
            if exempt_date_cutoff > date_object:
                staged.append(_buffer)
            else:
                not_staged.append(_buffer)
    return staged

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def are_dfs_equal(df1, df2):
    # Check if the schemas are the same
    if df1.schema != df2.schema:
        return False

    # Check if the content is the same
    if df1.subtract(df2).count() != 0 or df2.subtract(df1).count() != 0:
        return False

    return True

def get_duplicates(df):
    """ 
    this function find duplicate values in df, and returns a dataframe containing those duplicated values
    """
    return df.exceptAll(df.dropDuplicates()).cache()

def rename_labels(dictionary_mapping, df, col):
    mapping_expr = create_map([lit(x) for x in chain(*dictionary_mapping.items())])
    df = df.withColumn(col, mapping_expr[df[col]])
    return df.cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Isolate NLTK download to only when we need it
def prepare_word_filter_udf():
    # Download the NLTK words corpus
    nltk.download('words')

    # Broadcast to all executors
    english_words = set(nltk.corpus.words.words())
    broadcast_english_words = spark.sparkContext.broadcast(english_words)

    def contains_english_word(comment):
        # TODO: remove later once we are sure of the new approach
        # this approach doesn't work for certain scenarios where there is a punctuation, e.g. Extraordinary!
        # comment_words = set(comment.lower().split())

        # this approach is a bit more flexible, since it uses a tokenizer to find tokens (instead of words)
        tokenizer = nltk.tokenize.RegexpTokenizer(r'\w+') # alphanumeric characters
        comment_words = set(tokenizer.tokenize(comment.lower()))
        english_words_set = broadcast_english_words.value
        return not comment_words.isdisjoint(english_words_set)

    # Return the function for use in Intake
    return udf(contains_english_word, BooleanType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class QualityControlChecks:

    @staticmethod
    def gather_label_counts(df):
        """
        This method perform pivot-like sequential group by's to count distinct feedback id's for each label value in each label type
        The label types are hard coded to match parity with schema change requirements - when one changes so must they all
        
        Parameters:
        - df (DataFrame): FACT_FITGAI_prod without nonresponses or empty responses.
        Returns:
        - DataFrame: A dataframe containing the distinct counts responses from the latest processing of FITGAI prod
        """
        # Label columns should be hardcoded - if they change, both the reporting and QC tables will require reconstruction
        label_cols = ['SentimentLabel', 'WorkloadLabel', 'SupportLabel', 'PlatformLabel']

        df_list = []

        for col_name in label_cols:
            # Coalesce NULL values to 'Not Labeled' for ease of manipulation
            counts_df = df.groupBy(coalesce(col(col_name), lit("Not Labeled")).alias("LabelValue"))\
                            .agg(countDistinct("FeedbackId").alias("DistinctResponseCount"))\
                            .orderBy("LabelValue")

            counts_df = counts_df.withColumn("LabelType", lit(col_name)).withColumn("DIM_DateId",lit(runDateInt))
            counts_df = counts_df.select("DIM_DateId", "LabelType", "LabelValue", "DistinctResponseCount")
            df_list.append(counts_df)

        final_df = df_list[0] # Start with the first, then append the rest
        for temp_df in df_list[1:]:
            final_df = final_df.unionByName(temp_df)

        return final_df

    @staticmethod
    def compare_distinct_response_counts(prior_df, curr_df):
        """
        This method will apply monotonic behavioral condition checks between prior and current dataframes
        Parameters:
        - prior_df (DataFrame): The dataframe containing label counts from yesterdays run
        - curr_df (DataFrame): The dataframe containing label counts from todays run
        Returns:
        - DataFrame: A dataframe containing all columns of curr_df, with IssueFlag populated based on comparison with prior_df
        """
        # Prior must be on left to ensure persistance if the row is dropped in current
        joined_df = prior_df.alias("prior") \
            .join(curr_df.alias("current"), on=["LabelType", "LabelValue"], how="left")
        
        # Coalesce NULL to 0 if a label disappears entirely
        joined_df = joined_df.withColumn("current.DistinctResponseCount", coalesce(col("current.DistinctResponseCount"), lit(0)))

        result_df = joined_df.withColumn(
            "IssueFlag",
            when( 
                col("current.LabelValue") == lit("Not Labeled"), # If the label value is null
                when( 
                    col("current.DistinctResponseCount") <= col("prior.DistinctResponseCount"), # Ensure the behavior matches monotonic decrease
                    lit(False)  # Flag false to indicate no issue
                ).otherwise(lit(True))  # Flag true to indicate issue
            ).otherwise( # If the label value is not null
                when( 
                    col("current.DistinctResponseCount") >= col("prior.DistinctResponseCount"), # Ensure the behavior matches monotonic increase
                    lit(False)
                ).otherwise(lit(True)) 
            )
        )

        # Discard prior when returning df
        return result_df.select(
            col("current.DIM_DateId").alias("DIM_DateId"),
            col("current.LabelType").alias("LabelType"),
            col("current.LabelValue").alias("LabelValue"),
            col("current.DistinctResponseCount").alias("DistinctResponseCount"),
            col("IssueFlag")
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if external_deps:
    print("loading functions with external dependency")
    def find_closest(query, closed_list):
        # Find and return the closest match to the query in the closed list
        result = process.extractOne(query, closed_list)
        return result[0]
    
    def enforce_labels(row):
        # Enforce labels for sentiment, workload, platform, and support
        enforced_sentiment = find_closest(row.SentimentLabel, SENTIMENT_TYPE_LIST.split('\n')[1:-1])
        enforced_workload = find_closest(row.WorkloadLabel, workload_list)
        enforced_platform = find_closest(row.PlatformLabel, PLATFORM_HORIZONTAL_LIST.split('\n')[1:-1])
        enforced_support = find_closest(row.SupportLabel, SUPPORT_LIST.split('\n')[1:-1])   

        row_dict = row.asDict()
        cols = row_dict.keys()
        _labels = ["SentimentLabel", "WorkloadLabel", "PlatformLabel", "SupportLabel"] # TODO: pass from fix_label_variations

        _l = [row_dict[x] for x in cols if x not in _labels]
        _l.extend([enforced_sentiment, enforced_workload, enforced_platform, enforced_support])

        return_res = tuple(_l)
        return return_res

    def fix_label_variations(df, workload_list):
        # if df is empty, return df
        if df.count() == 0:
            return df
        # Apply enforce_labels function to each row of the input dataframe
        cols = df.columns
        _labels = ["SentimentLabel", "WorkloadLabel", "PlatformLabel", "SupportLabel"] # TODO: pass to enforce_labels
        schema = [x for x in cols if x not in _labels]
        schema.extend(_labels)

        new_rdd = df.rdd.map(enforce_labels)
        
        # Convert the resulting RDD back into a dataframe with updated column names and return it
        df_out = new_rdd.toDF(schema)
        return df_out.cache()

    def enforce_consensus(row):
        # Enforce consensus
        consensus_list = ["Disagree", "Agree"]
        # Consensus & Justification

        enforced_consensus = find_closest(row.Consensus, consensus_list)

        enforced_justification =  row.Justification
        remainder = row.Consensus.replace(enforced_consensus, "")

        def remove_string_from_start(_string, string_to_remove):
            if _string.startswith(string_to_remove):
                _string = _string[len(string_to_remove):]
            return _string

        # TODO: list comprehension
        string_to_remove = ". "
        remainder = remove_string_from_start(remainder, string_to_remove)

        string_to_remove = "."
        remainder = remove_string_from_start(remainder, string_to_remove)
        
        if enforced_justification=="-999":
            if len(remainder)>1:
                enforced_justification = remainder
            else:
                enforced_justification = " "
 
        row_dict = row.asDict()
        return_row = (row.SurveyComment, row.Segment, row_dict[original_label_col], \
                      row.error, enforced_consensus, enforced_justification)

        return return_row

    def fix_validation_variations(df, original_label_col):
        # Apply enforce_consensus function to each row of the input dataframe
        new_rdd = df.rdd.map(enforce_consensus)

        # Convert the resulting RDD back into a dataframe with updated column names and return it
        # Define the schema
        schema = StructType([
            StructField("SurveyComment", StringType(), True),
            StructField("Segment", StringType(), True),
            StructField(original_label_col, StringType(), True),
            StructField("error", StructType(), True),
            StructField("Consensus", StringType(), True),
            StructField("Justification", StringType(), True)
        ])

        # Assuming 'rdd' is your existing RDD
        # Apply the schema to the RDD and convert it to a DataFrame
        df_out = new_rdd.toDF(schema)
        return df_out.cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class IntakeFuzzyDeduplication:

    @staticmethod
    def make_duplicates_mapping(new_df: DataFrame, known_df: DataFrame, min_cosine_similarity=0.95, max_lev_distance=15, model_name="avsolatorio/GIST-Embedding-v0"):
        """
        This method identifies and maps duplicate comments from a new dataset to an existing dataset of known comments.

        Parameters:
        - new_df (DataFrame): The dataframe containing new comments that may have duplicates in the known comments. Must include a 'SurveyComment' column.
        - known_df (DataFrame): The dataframe containing already processed and reported comments. Must include a 'SurveyComment' column.
        - min_cosine_similarity (float, optional): The minimum cosine similarity score required to consider two comments as duplicates. Default is 0.95. A high value ensures that only very similar comments are considered duplicates.
        - max_lev_distance (int, optional): The maximum Levenshtein distance allowed between two comments to be considered duplicates. Default is 15. This allows for minor variations in the comments while still identifying them as duplicates.
        - model_name (str, optional): The name of the model used for generating comment embeddings. Default is "avsolatorio/GIST-Embedding-v0". 
                                      This model is chosen heuristically from the hf leaderboard: https://huggingface.co/spaces/mteb/leaderboard the model the is highest on the leaderboard that is small enough to run in a standard session.

        Returns:
        - DataFrame: A dataframe containing the mapping of new comments to their duplicates in the known comments, with columns 'SurveyComment', 'SurveyCommentDeduped', and 'ReportingDate'.
        """
        new_comments = new_df.select("SurveyComment").rdd.flatMap(lambda x: x).collect()
        known_comments = known_df.select("SurveyComment").rdd.flatMap(lambda x: x).collect()
        known_comments_count = len(known_comments)

        model = SentenceTransformer(model_name, trust_remote_code=True)       

        new_embeddings = model.encode(new_comments)
        known_embeddings = model.encode(known_comments)

        nbrs = NearestNeighbors(n_neighbors=min(5, known_comments_count), metric='cosine').fit(known_embeddings)
        distances, indices = nbrs.kneighbors(new_embeddings)

        mapping = []
        for i, new_comment in enumerate(new_comments):
            for j, idx in enumerate(indices[i]):
                known_comment = known_comments[idx]
                cosine_similarity = 1 - distances[i][j]
                if cosine_similarity >= min_cosine_similarity and \
                   lev.distance(new_comment, known_comment) <= max_lev_distance:
                   mapping.append((new_comment, known_comment, None))
                   break

        schema_duplicate_mapping = StructType([
            StructField("SurveyComment", StringType(), True),
            StructField("SurveyCommentDeduped", StringType(), True),
            StructField("ReportingDate", IntegerType(), True),
        ])
        
        mapping_df = spark.createDataFrame(mapping, schema=schema_duplicate_mapping)
        return mapping_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getPrincipalAccessibleId(resource_type):
    if resource_type not in ['capacities', 'workspaces']:
        raise ValueError("resource_type must be either 'capacities' or 'workspaces'")
    if resource_type == 'workspaces':
        search_string = "HelixFabric-Prod-DataScience"

    availableList = FabricEndpoint().invoke(method='GET', url=resource_type)

    df = spark.createDataFrame(availableList['value'])
    filtered_df = df.filter(col('displayName')==search_string)
    prod_ids = [row['id'] for row in filtered_df.collect()]
    return prod_ids

def getWorkspaceAccessibleItemId(workspace_id):
    #UDF is not yet an official item type in the docs June 9th 2025
    #https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype
    availableList = FabricEndpoint().invoke(method='GET', url='workspaces/{workspace_id}/items?')

    df = spark.createDataFrame(availableList['value'])
    filtered_df = df.filter(col('displayName').like('%UDF%'))
    prod_ids = [row['id'] for row in filtered_df.collect()]
    return prod_ids

#TODO use for UDF dynamic ID fetch
#print(getPrincipalAccessibleId("workspaces"))
#print(getWorkspaceAccessibleItemId(getPrincipalAccessibleId("workspaces")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class SearchSyncUtils:
    
    OrchestrationEnvironment = ""
    survey_type = ""
    workspaceId = "5ff40141-0d99-47e8-8510-df3b7abb262d"
    capacityId = "449CFD7E-7CEE-4A29-A330-AA882F5D579F"
    artifactId = "5bedd73a-2dec-4b5c-993e-9c31936faa3d"

    @staticmethod
    def get_database_rows(offset=0, page_size=1000):
        """
        Function to get all rows present in the search database and return as a dataframe.
        This function calls the getDbRows UDF which returns the db rows in a paginated way.
        """
        schema_db_rows = StructType([
            StructField("FeedbackId", StringType(), True),
            StructField("SegmentNumber", LongType(), True),
            StructField("ReportingDate", IntegerType(), True),
            StructField("SurveyType", StringType(), True) # Keep UserContext last for easier operations downstream
        ])

        rows = []
        while True:
            # TODO: get these params from Fabric API
            response = notebookutils.udf.run(
                workspaceId=SearchSyncUtils.workspaceId,
                capacityId=SearchSyncUtils.capacityId,
                artifactId=SearchSyncUtils.artifactId,
                functionName='getDbRows',
                parameters={
                    "environment": SearchSyncUtils.OrchestrationEnvironment,
                    "offset": offset,
                    "pageSize": page_size,
                    "surveyType": SearchSyncUtils.survey_type
                }
            )
            response =  json.loads(response)
            if len(response["errors"]) != 0:
                print(response["errors"])

            if len(response["output"]) == 0:
                break
            else:
                offset += page_size
                rows.extend(response["output"])
        
        if len(rows) == 0:
            return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_db_rows) 
        else:
            return spark.createDataFrame(rows, schema_db_rows)

    @staticmethod
    def batch(rows: list, batch_size: int):
        for i in range(0, len(rows), batch_size):
            yield rows[i:i + batch_size]

    @staticmethod
    def insert_rows(rows, batch_size=1000):
        rows_inserted = 0
        for batch_rows in SearchSyncUtils.batch(rows, batch_size):
            batch_rows = list(map(lambda row: row.asDict(), batch_rows))
            response = notebookutils.udf.run(
                workspaceId=SearchSyncUtils.workspaceId,
                capacityId=SearchSyncUtils.capacityId,
                artifactId=SearchSyncUtils.artifactId,
                functionName='insertDbRows',
                parameters={
                    "environment": SearchSyncUtils.OrchestrationEnvironment,
                    "rows": json.dumps(batch_rows)
                }
            )
            response = json.loads(response)
            if len(response["errors"]) != 0:
                print(response["errors"])
            rows_inserted += response["output"]
        return rows_inserted
    
    @staticmethod
    def delete_rows(rows, batch_size=1000):
        rows_deleted = 0
        for batch_rows in SearchSyncUtils.batch(rows, batch_size):
            batch_rows = list(map(lambda row: row.asDict(), batch_rows))
            response = notebookutils.udf.run(
                workspaceId=SearchSyncUtils.workspaceId,
                capacityId=SearchSyncUtils.capacityId,
                artifactId=SearchSyncUtils.artifactId,
                functionName='deleteDbRows',
                parameters={
                    "environment": SearchSyncUtils.OrchestrationEnvironment,
                    "rows": json.dumps(batch_rows)
                }
            )
            response = json.loads(response)
            if len(response["errors"]) != 0:
                print(response["errors"])
            rows_deleted += response["output"]
        return rows_deleted

    @staticmethod
    def update_rows(rows, batch_size=1000):
        rows_updated = 0
        for batch_rows in SearchSyncUtils.batch(rows, batch_size):
            batch_rows = list(map(lambda row: row.asDict(), batch_rows))
            response = notebookutils.udf.run(
                workspaceId=SearchSyncUtils.workspaceId,
                capacityId=SearchSyncUtils.capacityId,
                artifactId=SearchSyncUtils.artifactId,
                functionName='updateDbRows',
                parameters={
                    "environment": SearchSyncUtils.OrchestrationEnvironment,
                    "rows": json.dumps(batch_rows)
                }
            )
            response = json.loads(response)
            if len(response["errors"]) != 0:
                print(response["errors"])
            rows_updated += response["output"]
        return rows_updated
    
    @staticmethod
    def update_embeddings(batch_size=1000):
        response = notebookutils.udf.run(
            workspaceId=SearchSyncUtils.workspaceId,
            capacityId=SearchSyncUtils.capacityId,
            artifactId=SearchSyncUtils.artifactId,
            functionName='updateEmbeddings',
            parameters={
                "environment": SearchSyncUtils.OrchestrationEnvironment,
                "batchSize": batch_size
            }
        )
        response = json.loads(response)
        if len(response["errors"]) != 0:
            print(response["errors"])
        return response["output"]
    
    @staticmethod
    def delete_old_search_results(delta_seconds=24*60*60):
        notebookutils.udf.run(
            workspaceId='05aa7399-0bbd-450a-ada6-248857bb2ca2',
            capacityId='449CFD7E-7CEE-4A29-A330-AA882F5D579F',
            artifactId='6823642a-98a8-4a01-8c0b-64dea26afeb8',
            functionName='cleanupSearchResults',
            parameters={
                "environment": SearchSyncUtils.OrchestrationEnvironment,
                "deltaSeconds": delta_seconds
            }
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
