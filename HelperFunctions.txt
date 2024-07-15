# Databricks notebook source
import re, datetime, json, requests, warnings

# COMMAND ----------

def file_list_generator(bronze_storage_account, bronze_container, bronze_path, process_start_datetime, process_end_datetime):
  base_path = "abfss://{}@{}.dfs.core.windows.net/{}/".format(bronze_container, bronze_storage_account, bronze_path)

  file_list = []
  bronze_landing_datetime_list = []
  for day_count in range((process_end_datetime.date() - process_start_datetime.date()).days + 1):
    effective_date = process_start_datetime.date() + datetime.timedelta(days = day_count)
  
    try:
      for timestamp_folder in dbutils.fs.ls(base_path + effective_date.strftime('%Y%m%d')):
        bronze_landing_datetime_list.append(datetime.datetime.strptime(re.search('([0-9]{8}/[0-9]{6})', timestamp_folder.path).group(1), '%Y%m%d/%H%M%S'))
    except Exception as ex:
      if('path does not exist' in str(ex)):
        continue
      else:
        raise
        
  bronze_landing_datetime_list = sorted([landing_datetime for landing_datetime in bronze_landing_datetime_list if (landing_datetime >= process_start_datetime and landing_datetime < process_end_datetime)])
  for bronze_landing_datetime in bronze_landing_datetime_list:
    for file_object in dbutils.fs.ls(base_path + bronze_landing_datetime.strftime('%Y%m%d/%H%M%S')):
      file_list.append(file_object.path)
      
  return file_list

# COMMAND ----------

#This functions checks for files of this format <Filename>_<YYYYMMDD_HHMMSS>.<extension> in the specified paths, and selects one that fall within the processing range
def datetime_in_filename_read_regular_expression_generator(bronze_storage_account, bronze_container, bronze_path, process_start_datetime, process_end_datetime, fileExtension):
  base_path = "abfss://{}@{}.dfs.core.windows.net/{}/".format(bronze_container, bronze_storage_account, bronze_path)
  
  files_to_read = []
  for file_object in dbutils.fs.ls(base_path):
    bronze_landing_datetime = re.search('([0-9]{8}_[0-9]{6})\.'+fileExtension, file_object.name).group(1)
    if(bronze_landing_datetime >= process_start_datetime.strftime("%Y%m%d_%H%M%S") and
       bronze_landing_datetime < process_end_datetime.strftime("%Y%m%d_%H%M%S")
      ):
      files_to_read.append(file_object.name)
      
  data_read_string = base_path + '{' + ','.join(files_to_read) + '}'
  
  return data_read_string

def csv_mdm_file_read_regular_expression_generator(bronze_storage_account, bronze_container, bronze_path, process_start_datetime, process_end_datetime):
  base_path = "abfss://{}@{}.dfs.core.windows.net/{}/".format(bronze_container, bronze_storage_account, bronze_path)
  
  files_to_read = []
  for file_object in dbutils.fs.ls(base_path):
    bronze_landing_datetime = re.search('([0-9]{8}_[0-9]{6})\.csv', file_object.name).group(1)
    if(bronze_landing_datetime >= process_start_datetime.strftime("%Y%m%d_%H%M%S") and
       bronze_landing_datetime < process_end_datetime.strftime("%Y%m%d_%H%M%S")
      ):
      files_to_read.append(file_object.name)
      
  data_read_string = base_path + '{' + ','.join(files_to_read) + '}'
  
  return data_read_string

# COMMAND ----------

def file_read_regular_expression_generator(bronze_storage_account, bronze_container, bronze_path, process_start_datetime, process_end_datetime):
  base_path = "abfss://{}@{}.dfs.core.windows.net/{}/".format(bronze_container, bronze_storage_account, bronze_path)

  bronze_landing_datetime_list = []
  for day_count in range((process_end_datetime.date() - process_start_datetime.date()).days + 1):
    effective_date = process_start_datetime.date() + datetime.timedelta(days = day_count)

    try:
      for timestamp_folder in dbutils.fs.ls(base_path + effective_date.strftime('%Y%m%d')):
        bronze_landing_datetime = re.search('([0-9]{8}/[0-9]{6})', timestamp_folder.path).group(1)
        if(bronze_landing_datetime >= process_start_datetime.strftime('%Y%m%d/%H%M%S') and 
           bronze_landing_datetime < process_end_datetime.strftime('%Y%m%d/%H%M%S')):
          bronze_landing_datetime_list.append(bronze_landing_datetime)
    except Exception as ex:
      if('path does not exist' in str(ex)):
        continue
      else:
        raise
        
  data_read_string = base_path + '{' + ','.join(bronze_landing_datetime_list) + '}'
  
  return data_read_string

# COMMAND ----------

def get_pipeline_parameter_values(pipeline_id, get_parameter_property_api_url, debug_flag, job_id = None):
  parameter_group_code = ','.join([value for value in [pipeline_id, job_id] if value is not None])
  response = requests.post(get_parameter_property_api_url, json = {"ParameterGroupCode": parameter_group_code, 'p_Debug': debug_flag})
  
  if(response.status_code == 200):
    return json.loads(response.text)['message']
  else:
    raise ValueError(response.text)

# COMMAND ----------

def fixed_length_expression_builder(row, field_name_column, field_length_column, field_type_column):
  typeDict = {
    "CHAR": "string",
    "CURR": f"dec({int(row[field_length_column])-1}, {row['Decimal']})",
    "DATS": "date",
    "INT4": "int",
    "LANG": "string",
    "NUMC": "string",
    "CUKY": "string"
  }
  return f"cast(substring(value, {int(row['absolute_position']) + 1}, {int(row[field_length_column])}) as {typeDict[row[field_type_column]]}) as `{row[field_name_column]}`"

# COMMAND ----------

def parameter_recorder_write(path: str, parameter_dict: dict):
  
  #Archiving already exisiting table
  try:
    file_list = dbutils.fs.ls(path)
    for file in file_list:
      if(file.path.endswith('.parquet')):
        dbutils.fs.mv(file.path, file.path.replace(path, path + '/archive/'))
  except Exception as ex:
    if('does not exist' not in str(ex)):
      raise
    
  df = spark.createDataFrame([tuple(parameter_dict.values())], list(parameter_dict.keys()))
  df.write.mode('append').parquet(path)
  
  return True

# COMMAND ----------

def parameter_recorder_read(path: str):
  
  df = spark.read.parquet(path)
  return df.toPandas().to_dict('records')[0]

# COMMAND ----------

def synapse_ingestion(df, write_out_mode, sql_db_name, sql_db_table_schema, sql_db_table_name, synapse_resource_name, synapse_data_lake_storage_account, synapse_data_lake_container, synapse_data_lake_initial_folder):
  
  (df.write
   .mode(write_out_mode)
   .format("com.databricks.spark.sqldw")
   .option("preActions", f"TRUNCATE TABLE {sql_db_table_schema}.{sql_db_table_name}")
   .option("url", f"jdbc:sqlserver://{synapse_resource_name}.sql.azuresynapse.net:1433;database={sql_db_name};encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;Authentication=ActiveDirectoryIntegrated")
   .option("enableServicePrincipalAuth", "true")
   .option("dbTable", f"{sql_db_table_schema}.{sql_db_table_name}")
   .option("tempDir", f"abfss://{synapse_data_lake_container}@{synapse_data_lake_storage_account}.dfs.core.windows.net/DatabricksCopyStaging/{synapse_data_lake_initial_folder}/{sql_db_table_schema}_{sql_db_table_name}")
   .save()
  )
  
  return True
