# Databricks notebook source
# MAGIC %run "/Configuration"

# COMMAND ----------

# MAGIC %run "/HelperFunctions"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import datetime

# COMMAND ----------

dbutils.widgets.text("pipelineID", "", "pipelineID")
dbutils.widgets.text("jobID", "", "jobID")
dbutils.widgets.text("debugFlag", "", "debugFlag")
dbutils.widgets.text("getParameterPropertyApiUrl", "", "getParameterPropertyApiUrl")
dbutils.widgets.text("processStartDateTime", "", "processStartDateTime")
dbutils.widgets.text("processEndDateTime", "", "processEndDateTime")

pipeline_id = dbutils.widgets.get("pipelineID")
job_id = dbutils.widgets.get("jobID")
debug_flag = dbutils.widgets.get("debugFlag")
get_parameter_property_api_url = dbutils.widgets.get("getParameterPropertyApiUrl")
process_start_datetime = datetime.datetime.strptime(dbutils.widgets.get("processStartDateTime"), "%Y-%m-%dT%H:%M:%S")
process_end_datetime = datetime.datetime.strptime(dbutils.widgets.get("processEndDateTime"), "%Y-%m-%dT%H:%M:%S")

#Getting remaining pipeline parameters
parameter_dictionary = get_pipeline_parameter_values(pipeline_id, get_parameter_property_api_url, debug_flag, job_id)
bronze_storage_account = parameter_dictionary['bronzeStorageAccount']
bronze_container = parameter_dictionary['bronzeContainer']
bronze_path = parameter_dictionary['bronzePath']
silver_storage_account = parameter_dictionary['silverStorageAccount']
silver_raw_container = parameter_dictionary['silverRawContainer']
silver_raw_path = parameter_dictionary['silverRawPath']
business_key_columns = parameter_dictionary['businessKeyColumns']
modification_date_time_column = parameter_dictionary['modificationDateTimeColumn']

secret_scope_name = None
if('secretScopeName' in parameter_dictionary):
  secret_scope_name = parameter_dictionary['secretScopeName']

#Capturing deletes for full loads
capture_delete_flag = False
if('captureDeleteFlag' in parameter_dictionary):
  capture_delete_flag = bool(int(parameter_dictionary['captureDeleteFlag']))

#bypassing missing files failure with a flag
bypass_missing_files_failure = False
if('bypassMissingFilesFailure' in parameter_dictionary):
  bypass_missing_files_failure = bool(int(parameter_dictionary['bypassMissingFilesFailure']))
  
#Connecting to data lake with SPN during the transition period. 
#The solutuion assumes the connection is established with access key unless client Id and client secret key vault secret references are provided.
if('dataLakeServicePrincipalClientIdSecretReference' in parameter_dictionary and
   'dataLakeServicePrincipalClientSecretSecretReference' in parameter_dictionary and 
   'tenantIdSecretReference' in parameter_dictionary
  ):
  dataLakeServicePrincipalConnectionInitiation(parameter_dictionary['dataLakeServicePrincipalClientIdSecretReference'], parameter_dictionary['dataLakeServicePrincipalClientSecretSecretReference'], parameter_dictionary['tenantIdSecretReference'],secret_scope_name)

# COMMAND ----------

# MAGIC %run "../ScdTypeIIClass1.0.0"

# COMMAND ----------

data_read_string = file_read_regular_expression_generator(bronze_storage_account, bronze_container, bronze_path, process_start_datetime, process_end_datetime)

if(data_read_string.endswith('{}') and
   bypass_missing_files_failure
  ):
  dbutils.notebook.exit('No file found between processStartDateTime and procesEndDateTime')
  
else:
  parquet_dataframe = spark.read.parquet(data_read_string)

  scd_update = SCDTypeII(business_key_columns = business_key_columns,
                         modification_date_time_column = modification_date_time_column,
                         source_path = "abfss://{}@{}.dfs.core.windows.net/{}/".format(silver_raw_container, silver_storage_account, silver_raw_path),
                         update_dataframe = parquet_dataframe,
                         provided_file_path_flag = False,
                         capture_delete_flag = capture_delete_flag
                        )
  scd_update.scd_type_II_merge()