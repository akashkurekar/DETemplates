# Databricks notebook source
# MAGIC %md
# MAGIC # Moving Excel files from Bronze to Silver_Raw
# MAGIC
# MAGIC **Layers:** Bronze and Silver_Raw
# MAGIC
# MAGIC **Current Version:** Initial 1.0.0
# MAGIC
# MAGIC **Description:** This notebook is used in a similar fashion as the csv and parquet ingestions but for the excel file format.
# MAGIC
# MAGIC **Note:**
# MAGIC This notebook uses the same schema repository and same helper functions/configurations files as the parquet and csv files but is slightly different.
# MAGIC <br>
# MAGIC There is an addition to the job manager parameters, called the 'excelDataAddress' which allows you to specify which sheet, cell column and row you want to read from and to.
# MAGIC <br>
# MAGIC The notebook reads in the bronze file into a pandas on spark dataframe. (Not a pandas dataframe but a pandas on spark dataframe)
# MAGIC <br>
# MAGIC The difference is that pandas on spark is distributed like we know spark, and pandas is on a single machine which loses parallel processing.
# MAGIC <br>
# MAGIC The pandas on spark dataframe is used for its capability of being able to read an Excel file into a pandas-on-Spark DataFrame or Series and addition functionalities.
# MAGIC <br>
# MAGIC Then from the pandas on spark dataframe, we then change it to a spark dataframe to prepare for the SCDTypeII.
# MAGIC <br>
# MAGIC Another addition to the code was taking out special characters in the column names like spaces ' ', forwards slashes '/', and periods '.' into underscores.
# MAGIC <br>
# MAGIC From that point on, it does what the parquet and csv files do, the SCDTypeII changes, updates and then stores it into Silver_Raw.
# MAGIC
# MAGIC **Revision History:**
# MAGIC
# MAGIC | Version | Date | Description | Modified By |
# MAGIC |:----:|--------------|--------|----------------|
# MAGIC | 1.0 | Mar 27, 2023 | Initial development for local jupyter notebook version | Akash (akurekar@suncor.com)  Dominic (donguyen@susncor.com)  Oliver (oliong@suncor.com) |
# MAGIC
# MAGIC
# MAGIC **Contact:** Akash (akurekar@suncor.com)  Dominic (donguyen@susncor.com)  Oliver (oliong@suncor.com)

# COMMAND ----------

# MAGIC %run "/Configuration"

# COMMAND ----------

# MAGIC %run "/HelperFunctions"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import *
import datetime
import pyspark.pandas as pp
import numpy as np
from functools import reduce

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
file_name = parameter_dictionary['fileName']
schema_name = parameter_dictionary['schemaName']
excel_data_address = parameter_dictionary['excelDataAddress']
bronze_storage_account = parameter_dictionary['bronzeStorageAccount']
bronze_container = parameter_dictionary['bronzeContainer']
bronze_path = parameter_dictionary['bronzePath']
silver_storage_account = parameter_dictionary['silverStorageAccount']
silver_raw_container = parameter_dictionary['silverRawContainer']
silver_raw_path = parameter_dictionary['silverRawPath']
business_key_columns = parameter_dictionary['businessKeyColumns']
modification_date_time_column = parameter_dictionary['modificationDateTimeColumn']

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
  dataLakeServicePrincipalConnectionInitiation(parameter_dictionary['dataLakeServicePrincipalClientIdSecretReference'], parameter_dictionary['dataLakeServicePrincipalClientSecretSecretReference'], parameter_dictionary['tenantIdSecretReference'])


# COMMAND ----------

# MAGIC %run "./SchemaRepository"

# COMMAND ----------

# MAGIC %run "../ScdTypeIIClass1.0.0"

# COMMAND ----------

bronze_path = bronze_path + "/" + file_name
data_read_string = file_read_regular_expression_generator(bronze_storage_account, bronze_container, bronze_path, process_start_datetime, process_end_datetime)

if(data_read_string.endswith('{}') and
   bypass_missing_files_failure
  ):
  dbutils.notebook.exit('No file found between processStartDateTime and procesEndDateTime')

else: 
    file_path_list = file_list_generator(bronze_storage_account, bronze_container, bronze_path, process_start_datetime, process_end_datetime)
    df_list = []
    for file in file_path_list:
        df = pp.read_excel(file, sheet_name = excel_data_address, dtype=schema_repository[schema_name]) #pandas-on-spark DF
        init_sdf = df.to_spark().withColumn('__input_file_name', lit(file).cast('string')) #pyspark DF
        df_list.append(init_sdf)

    df = reduce(DataFrame.unionAll, df_list)

    # Replace invalid column characters
    for each in df.columns:
        df = df.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace(' ', '_')))
    for each in df.columns:
        df = df.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace('.', '_')))
    for each in df.columns:
        df = df.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace('/', '_')))


    scd_update = SCDTypeII(business_key_columns = str(business_key_columns),
                            modification_date_time_column = modification_date_time_column,
                            source_path = "abfss://{}@{}.dfs.core.windows.net/{}/".format(silver_raw_container, silver_storage_account, silver_raw_path),
                            update_dataframe = df,
                            provided_file_path_flag = True,
                            capture_delete_flag = capture_delete_flag
    )
    scd_update.scd_type_II_merge()  
