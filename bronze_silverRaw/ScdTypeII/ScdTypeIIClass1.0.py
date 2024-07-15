# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta import *
import datetime

# COMMAND ----------

class SCDTypeII: 
  def __init__(self, 
               business_key_columns: str,
               modification_date_time_column: str,
               source_path: str,
               update_dataframe: DataFrame = None,
               provided_file_path_flag: bool = False,
               is_historical: bool = False,
               capture_delete_flag: bool = False,
               modification_date_time_logic: str = None
              ):
    """
    Initiating the SCDTypeII object
    
    Args:
      business_key_columns: list of business keys 
      content_key_columns: list of content keys columns 
      modification_date_time_column: column name for the modification datetime column
      source_path: the path to the initial delta table that we're aiming to update
      update_dataframe: dataframe that would be updating the source delta table
      provided_file_path_flag: determines whether the file path for the row content is provided in the dataframe
      modification_date_time_logic: if modification_date_time_column doesn't exist or is not enough for sorting records, this string value can represent a logic that generates a column
      of TimestampType.
      capture_delete_flag: determines whether this delta table tracks deleted records
    """
    
    self.business_key_columns = business_key_columns.split(',')
    self.modification_date_time_column = modification_date_time_column
    self.source_path = source_path
    if('__input_file_name' not in update_dataframe.columns):
      self.update_dataframe = update_dataframe.withColumn('__input_file_name', lit(None).cast(StringType()))
    else:
      self.update_dataframe = update_dataframe
    #if __input_file_name column is provided externally, it should not be part of data content (mostly applicable to excrypted xml ingestion)
    self.content_key_columns = sorted([column_name for column_name in self.update_dataframe.columns if column_name != '__input_file_name'], key = lambda element : element.lower())
    self.provided_file_path_flag = provided_file_path_flag
    self.capture_delete_flag = capture_delete_flag
    self.is_historical = is_historical
    self.modification_date_time_logic = modification_date_time_logic
    self.process_date_time = datetime.datetime.now()
    
    # making sure source delta table exists or creating it.
    try:
      self.source_dataframe = spark.read.format('delta').load(self.source_path)
    except Exception as ex:
      if("is not a Delta table." in str(ex)):
        #if __input_file_name column is provided externally, it should not be part of data content (mostly applicable to excrypted xml ingestion)
        default_schema = self.update_dataframe.drop('__input_file_name').schema
        self.source_dataframe = (spark
                                 .createDataFrame([], schema = default_schema)
                                 .withColumn('__current', lit(None).cast('boolean'))
                                 .withColumn('__deleted', lit(None).cast('boolean'))
                                 .withColumn('__activation_datetime', lit(None).cast('timestamp'))
                                 .withColumn('__deactivation_datetime', lit(None).cast('timestamp'))
                                 .withColumn('__business_key_hash', lit(None).cast('string'))
                                 .withColumn('__content_key_hash', lit(None).cast('string'))
                                 .withColumn('__is_historical', lit(None).cast('boolean'))
                                 .withColumn('__process_datetime', lit(None).cast('timestamp'))
                                )
        self.source_dataframe.write.format('delta').partitionBy('__current').save(self.source_path)
  
  def _schema_evolution_handler(self):
    update_dataframe_data_columns = [column_name for column_name in self.update_dataframe.columns if not column_name.startswith('__')]
    source_dataframe_data_columns = [column_name for column_name in self.source_dataframe.columns if not column_name.startswith('__')]
    
    extra_columns_set = set(update_dataframe_data_columns) - set(source_dataframe_data_columns)
    update_table = 0
    if(len(extra_columns_set) != 0): 
      #Means there are columns in updating dataframe that are not in source dataframe
      #So we need to add the column to the silver raw table
      print('Schema Evolution In Progress')
      
      #Adding extra columns source dataframe
      for column_name in extra_columns_set:
        self.source_dataframe = self.source_dataframe.withColumn(column_name, lit(None).cast(self.update_dataframe.schema[column_name].dataType))
      
      #using self.content_key_columns to arrange self.source_dataframe columns to make sure __content_key_hash is generated consistently from order of columns perspective.
      audit_columns = [column_name for column_name in self.source_dataframe.columns if column_name.startswith('__')]
      self.source_dataframe = (self.source_dataframe
                               .withColumn('__content_key_hash', sha2(concat_ws('|', *[col(column).cast(StringType()) for column in self.content_key_columns]), 256))
                               .select(self.content_key_columns + audit_columns)
                              )
      update_table = 1
    
    #Adding new audit columns to source dataframe
    if('__is_historical' not in scd_update.source_dataframe.columns):
        scd_update.source_dataframe = scd_update.source_dataframe.withColumn('__is_historical', f.lit(scd_update.is_historical))
        update_table = 1
    if('__deleted' not in scd_update.source_dataframe.columns):
        scd_update.source_dataframe = scd_update.source_dataframe.withColumn('__deleted', lit(False))
        update_table = 1

    #overwriting silver raw table
    if(update_table == 1):
        self.source_dataframe.write.mode('overwrite').option("mergeSchema", True).format('delta').save(self.source_path)
      
    return True
  
  def _add_helper_columns_to_update(self):
    """ Stage one preparation on current table before merge. Adding SCD_COLS to other herlper functions to aid table preparations. 
    Deduplicates table on content key hash where duplicates exist within the same '__bronze_landing_date_time'.
    Also identifies which record should have the current flag enabled.
        
    Returns:
        DataFrame: current dataframe with helper functions added.
    """    
    data_columns = self.content_key_columns + ['__current', '__deleted', '__activation_datetime', '__modification_date_time', '__business_key_hash', '__content_key_hash', '__is_historical', '__process_datetime']
    final_data_columns = self.content_key_columns + ['__current', '__deleted', '__activation_datetime', '__deactivation_datetime', '__minimum_activation_date', '__business_key_hash', '__content_key_hash', '__is_historical', '__process_datetime']
    
    if(self.provided_file_path_flag):
      self.initial_preparation = (self.update_dataframe
                                  .withColumn('__input_file_name', col('__input_file_name'))
                                  .withColumn('__bronze_landing_date_time', to_timestamp(regexp_replace(regexp_extract(col('__input_file_name'), '([0-9]{8}[/_][0-9]{6})', 1), '_', '/'), 'yyyyMMdd/HHmmss'))

                                  .withColumn('__business_key_hash', sha2(concat_ws('|', *self.business_key_columns), 256))
                                  .withColumn('__content_key_hash', sha2(concat_ws('|', *[col(column).cast(StringType()) for column in self.content_key_columns]), 256))

                                  # take the first non-None value in [self.modification_date_time_logic, self.modification_date_time_column] and use it to generate __modification_date_time
                                  .withColumn('__modification_date_time', expr(next(item for item in [self.modification_date_time_logic, self.modification_date_time_column] if item is not None)))
                                 )
    else:
      self.initial_preparation = (self.update_dataframe
                                  .withColumn('__input_file_name', input_file_name())
                                  .withColumn('__bronze_landing_date_time', to_timestamp(regexp_replace(regexp_extract(col('__input_file_name'), '([0-9]{8}[/_][0-9]{6})', 1), '_', '/'), 'yyyyMMdd/HHmmss'))

                                  .withColumn('__business_key_hash', sha2(concat_ws('|', *self.business_key_columns), 256))
                                  .withColumn('__content_key_hash', sha2(concat_ws('|', *[col(column).cast(StringType()) for column in self.content_key_columns]), 256))

                                  # take the first non-None value in [self.modification_date_time_logic, self.modification_date_time_column] and use it to generate __modification_date_time
                                  .withColumn('__modification_date_time', expr(next(item for item in [self.modification_date_time_logic, self.modification_date_time_column] if item is not None)))
                                 )
    
    deduplication_window = Window.partitionBy('__content_key_hash').orderBy(col('__bronze_landing_date_time').desc())
    current_window_spec = Window.partitionBy('__business_key_hash').orderBy(col('__modification_date_time').desc())
    bkh_window_spec = Window.partitionBy('__business_key_hash')
    
    self.update_dataframe_init = (self.initial_preparation
                                  # Retrieve most recent record for each BK
                                  .withColumn('__bronze_landing_date_time_row_number', row_number().over(current_window_spec))
                                  .where(col('__bronze_landing_date_time_row_number') == 1))
    
    self.update_dataframe_curr = (self.update_dataframe_init
                                  # Isolate new records i.e. new content not new business keys 
                                  # Only want to retrieve the most current records that are not already present in source_dataframe.
                                  .join(self.source_dataframe.alias("original").filter(col('__current')==True).filter(col('__deleted')==False),'__content_key_hash', 'left_anti')

                                  # Remove duplicate records with the same contents
                                  .withColumn('__bronze_landing_date_time_row_number', row_number().over(deduplication_window))
                                  .where(col('__bronze_landing_date_time_row_number') == 1)

                                  .withColumn('__activation_datetime', col('__modification_date_time'))

                                  .withColumn('__modified_date_time_row_number', row_number().over(current_window_spec))                
                                  .withColumn('__current', when(col('__modified_date_time_row_number') == 1, lit(True)).otherwise(lit(False)))
                                  .withColumn('__deleted', lit(False))

                                  .withColumn('__is_historical', f.lit(self.is_historical))
                                  .withColumn('__process_datetime', lit(self.process_date_time))
                                  .select(*data_columns)
                                  .cache())
    
    self.update_dataframe_history = (self.initial_preparation.alias('init')
                                     # Ignore all records that match the contents of the most recent record obtained in the previous DF
                                     .join(self.update_dataframe_init.alias('curr'),'__content_key_hash','left_anti')
                                     
                                     # Only retrieve records that are not already present in source_dataframe unless content has been modified more recently than the already present record.
                                     .join(self.source_dataframe.alias("original"),((col('init.__content_key_hash')==col('original.__content_key_hash')) & (col('init.__modification_date_time')<= col('original.__activation_datetime'))), 'left_anti')

                                     # Remove duplicate records with the same contents
                                     .withColumn('__bronze_landing_date_time_row_number', row_number().over(deduplication_window))
                                     .where(col('__bronze_landing_date_time_row_number') == 1)

                                     .withColumn('__activation_datetime', col('init.__modification_date_time'))

                                     .withColumn('__modified_date_time_row_number', row_number().over(current_window_spec))                
                                     .withColumn('__current', lit(False))
                                     .withColumn('__deleted', lit(False))

                                     .withColumn('__is_historical', f.lit(self.is_historical))
                                     .withColumn('__process_datetime', lit(self.process_date_time))
                                     .select(*data_columns)
                                     .cache())

    # Merge both curr and history dataframes and add deactivation datetime column based on activation datetime of newer records
    self.update_dataframe = (self.update_dataframe_curr
                             .union(self.update_dataframe_history)
                             .withColumn('__deactivation_datetime', lag(col('__activation_datetime'), 1).over(current_window_spec))
                             .withColumn('__minimum_activation_date',min(col('__modification_date_time')).over(bkh_window_spec))
                             .select(*final_data_columns))
    
    print("number of new record contents: " + str(self.update_dataframe.count()))
    
    return True
    
  def _scd_type_II_merge_setup(self):
    """ Stage the update by unioning two sets of records. 
    1. New records (records with new business keys) to be INSERTED and records that would be used to UPDATE currently existing records aka UPSERT records
    2. Records that would UPDATE the currently existing records (records whose business keys exist in our source table)
    
    Returns:
        DataFrame: Dataframe ready for merge operation.
    """
    
    self._schema_evolution_handler()
    self._add_helper_columns_to_update()
    
    staged_update = (self.update_dataframe.alias("update")
                     .withColumn('__mergeKey', lit(None))
                     .unionByName(self.update_dataframe.alias("update")
                                  .join(self.source_dataframe.alias("original").where(col('__current')),'__business_key_hash')
                                  .withColumn('__mergeKey', when(col('update.__current') & ((col('original.__content_key_hash') != col('update.__content_key_hash')) | (col('original.__deleted')==True)) & (f.col('original.__activation_datetime') <= f.col('update.__activation_datetime')), 
                                                                 col('__business_key_hash')))
                                  .where(col('__mergeKey').isNotNull())
                                  .select('update.*', '__mergeKey')))
      
    
    return staged_update
  
  def scd_type_II_merge(self):
    """ Apply SCD Type 2 operation using merge. 
    1. Inserts the new records with its current flag set to true
    2. Updates the previous current row to set current to false, and update the '__deactivation_datetime' from null to the '__activation_datetime' from the source.
        
    Returns:
        DataFrame: Dataframe ready for merge operation.
    """
    staged_update = self._scd_type_II_merge_setup()
    source_dataframe = DeltaTable.forPath(spark, self.source_path)
    
    (source_dataframe.alias("original")
     .merge(source = staged_update.alias("update"), 
            condition = expr("original.__business_key_hash = update.__mergeKey"))
     .whenMatchedUpdate(condition = expr("original.__current = true AND (original.__content_key_hash <> update.__content_key_hash OR original.__deleted = true)"), 
                        set = {'__current' : lit(False),
                               '__deactivation_datetime' : col("update.__minimum_activation_date"),
                               '__process_datetime': lit(self.process_date_time)
                               })
     .whenNotMatchedInsertAll()
     .execute())
    
    if(self.capture_delete_flag):
      source_dataframe = DeltaTable.forPath(spark, self.source_path)
      remaining_business_keys = (self.initial_preparation.select('__business_key_hash', '__bronze_landing_date_time')
                                 .withColumn('__max_bronze_landing_date_time', max(col('__bronze_landing_date_time')).over(Window.partitionBy()))
                                 .where(col('__max_bronze_landing_date_time') == col('__bronze_landing_date_time'))
                                 .select('__business_key_hash')
                                )
      self.deleted_records = (source_dataframe.toDF().where((col('__current')) & (~col('__is_historical'))).select('__business_key_hash')
                              .join(remaining_business_keys, '__business_key_hash', 'left_anti')
                             )
      
      (source_dataframe.alias('original')
       .merge(source = self.deleted_records.alias('deleted'),
              condition = expr("original.__business_key_hash = deleted.__business_key_hash")
             )
       .whenMatchedUpdate(condition = expr("original.__current = true"),
                          set = {'__current': lit(True),
                                 '__deleted': lit(True),
                                 '__deactivation_datetime': coalesce(col('original.__deactivation_datetime'), lit(self.process_date_time)),
                                 '__process_datetime': lit(self.process_date_time)
                                 })
       .execute())
    
    return True
    
  def get_delta_change_data_feed_log(self, 
                                     versions: list = None):
    """ View counts on change events after merge operation. This function should be called after the merge operation.
    Returns information about the number of updated rows, number of deleted rows, number of inserted rows and number of affected rows.
    
    Args:
        version: delta table version
        
    Returns: change feed dataframe containing quantitative data about merge operation
    """
    if(versions is None):
      delta = DeltaTable.forPath(spark, self.source_path)
      delta_version = (delta
                       .history()
                       .where(col('operation') == 'MERGE')
                       .orderBy(col('version').desc())
                      )

      if(self.capture_delete_flag):
        interesting_versions = [element.version for element in delta_version.collect()[0:2]]
      else:
        interesting_versions = [element.version for element in delta_version.collect()[0:1]]
    
    else:
      interesting_versions = versions

    change_log = (delta
                  .history()
                  .where(col('version').isin(interesting_versions))
                  .withColumn('version', col('version'))
                  .withColumn('change_timestamp', col('timestamp'))
                  .withColumn('operation', col('operation'))
                  .withColumn('execution_time_in_ms', col('operationMetrics.executionTimeMs'))
                  .withColumn('num_deleted_rows', col('operationMetrics.numTargetRowsDeleted'))
                  .withColumn('num_updated_rows', col('operationMetrics.numTargetRowsUpdated'))
                  .withColumn('num_inserted_rows', col('operationMetrics.numTargetRowsInserted'))
                  .select('version', 'change_timestamp', 'operation', 'execution_time_in_ms', 'num_deleted_rows', 'num_updated_rows', 'num_inserted_rows'))

    return change_log