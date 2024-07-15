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
schema_name = parameter_dictionary['schemaName']
csv_header_flag = parameter_dictionary['csvHeaderFlag']
csv_delimiter = parameter_dictionary['csvDelimiter']
business_key_columns = parameter_dictionary['businessKeyColumns']
modification_date_time_column = parameter_dictionary['modificationDateTimeColumn'] #If source doesn't have a column for this then we could use __bronze_landing_date_time

secret_scope_name = None
if('secretScopeName' in parameter_dictionary):
  secret_scope_name = parameter_dictionary['secretScopeName']

#Capturing deletes for full loads
capture_delete_flag = False
if('captureDeleteFlag' in parameter_dictionary):
  capture_delete_flag = bool(int(parameter_dictionary['captureDeleteFlag']))

#Bypassing missing files failure with a flag
bypass_missing_files_failure = False
if('bypassMissingFilesFailure' in parameter_dictionary):
  bypass_missing_files_failure = bool(int(parameter_dictionary['bypassMissingFilesFailure']))

#Datetimestamp in filename
file_has_timestamp = False
if('fileHasTimestamp' in parameter_dictionary):
  file_has_timestamp = bool(int(parameter_dictionary['fileHasTimestamp']))
timestamp_format = None
if('timestampFormat' in parameter_dictionary):
  timestamp_format = parameter_dictionary['timestampFormat']

#Connecting to data lake with SPN during the transition period.
#The solutuion assumes the connection is established with access key unless client Id and client secret key vault secret references are provided.
if('dataLakeServicePrincipalClientIdSecretReference' in parameter_dictionary and
   'dataLakeServicePrincipalClientSecretSecretReference' in parameter_dictionary and
   'tenantIdSecretReference' in parameter_dictionary
  ):
  dataLakeServicePrincipalConnectionInitiation(parameter_dictionary['dataLakeServicePrincipalClientIdSecretReference'], parameter_dictionary['dataLakeServicePrincipalClientSecretSecretReference'], parameter_dictionary['tenantIdSecretReference'], secret_scope_name)

# COMMAND ----------

# MAGIC %run "./SchemaRepository"

# COMMAND ----------

# MAGIC %run "../ScdTypeIIClass1.0.0"

# COMMAND ----------

data_read_string = None
if(file_has_timestamp):
    data_read_string = datetime_in_filename_read_regular_expression_generator(bronze_storage_account, bronze_container, bronze_path, process_start_datetime, process_end_datetime, 'csv')
else:
    data_read_string = file_read_regular_expression_generator(bronze_storage_account, bronze_container, bronze_path, process_start_datetime, process_end_datetime)

if(data_read_string.endswith('{}') and
   bypass_missing_files_failure
  ):
    dbutils.notebook.exit('No file found between processStartDateTime and procesEndDateTime')

else:
    csv_dataframe = None
    # if schema name is provided, read the files using the schema name from the schema repository
    if(timestamp_format is not None):
        csv_dataframe = spark.read.option('header', csv_header_flag).option("timestampFormat", timestamp_format).option('sep', csv_delimiter).schema(schema_repository[schema_name]).csv(data_read_string)
    else:
        csv_dataframe = spark.read.option('header', csv_header_flag).option('sep', csv_delimiter).schema(schema_repository[schema_name]).csv(data_read_string)

    scd_update = SCDTypeII(business_key_columns = business_key_columns,
                         modification_date_time_column = modification_date_time_column,
                         source_path = "abfss://{}@{}.dfs.core.windows.net/{}/".format(silver_raw_container, silver_storage_account, silver_raw_path),
                         update_dataframe = csv_dataframe,
                         provided_file_path_flag = False,
                         capture_delete_flag = capture_delete_flag
                        )
    scd_update.scd_type_II_merge()