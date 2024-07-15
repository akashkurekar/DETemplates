# Databricks notebook source
import json

# COMMAND ----------

def dataLakeServicePrincipalConnectionInitiation(data_lake_service_principal_client_id_secret_reference, data_lake_service_principal_client_secret_secret_reference, tenant_id_secret_reference,secretScopeName = None):
    if(secretScopeName == None):
        secretScopeName = 'edwarmkeyuw2001'
    spark.conf.set("fs.azure.account.auth.type", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id", dbutils.secrets.get(scope = secretScopeName, key = data_lake_service_principal_client_id_secret_reference))
    spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope = secretScopeName, key = data_lake_service_principal_client_secret_secret_reference))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{dbutils.secrets.get(scope = secretScopeName, key = tenant_id_secret_reference)}/oauth2/token")

    return True