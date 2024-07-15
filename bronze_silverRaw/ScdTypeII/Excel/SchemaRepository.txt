# Databricks notebook source
from pyspark.sql.types import *
import numpy as np

# COMMAND ----------

schema_repository = {
    'EH_Hours_Adj' : {
        'Organizational Unit.Level 02.Key':np.float64,
        'Organizational Unit.Level 02.Medium Name':str,
        'Organizational Unit.Level 03.Key':np.float64,
        'Organizational Unit.Level 03.Medium Name':str,
        'Organizational Unit.Level 04.Key':np.float64,
        'Organizational Unit.Level 04.Medium Name':str,
        'Organizational Unit.Level 05.Key':np.float64,
        'Organizational Unit.Level 05.Medium Name':str,
        'Organizational Unit.Level 06.Key':np.float64,
        'Organizational Unit.Level 06.Medium Name':str,
        'Organizational Unit.Level 07.Key':np.float64,
        'Organizational Unit.Level 07.Medium Name':str,
        'Organizational Unit.Level 08.Key':np.float64,
        'Organizational Unit.Level 08.Medium Name':str,
        'Organizational Unit.Level 09.Key':np.float64,
        'Organizational Unit.Level 09.Medium Name':str,
        'Employee Group.Employee Group Level 01':str,
        'Org ID':np.int64,
        'Org Name':str,
        'Actual Time':np.float64,
        'Year/Month':str,
        'Primary Key':str
    }
}
