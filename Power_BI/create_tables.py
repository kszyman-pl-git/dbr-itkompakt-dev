# Databricks notebook source
key_vault_scope = "kv-cdh-launchpad-da-72"

service_id = dbutils.secrets.get(scope=key_vault_scope, key="az-sp-extended-devops-app-da-72dv-ID")
service_key = dbutils.secrets.get(scope=key_vault_scope, key="az-sp-extended-devops-app-da-72dv-KEY")
pg_tenant_id = dbutils.secrets.get(scope=key_vault_scope, key="tenant-id")

# print(service_id)

spark.conf.set("fs.azure.account.auth.type.blobcdhlaunchpadda72dv.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.blobcdhlaunchpadda72dv.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.blobcdhlaunchpadda72dv.dfs.core.windows.net", service_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.blobcdhlaunchpadda72dv.dfs.core.windows.net", service_key)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.blobcdhlaunchpadda72dv.dfs.core.windows.net", f"https://login.microsoftonline.com/{pg_tenant_id}/oauth2/token")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists hive_metastore.stc_tech.workspace_list;
# MAGIC drop table if exists hive_metastore.stc_tech.dataset_list;
# MAGIC drop table if exists hive_metastore.stc_tech.report_list;
# MAGIC

# COMMAND ----------

dbutils.fs.rm('abfss://stc-tech@blobcdhlaunchpadda72dv.dfs.core.windows.net/workspace_list/', True)
dbutils.fs.rm('abfss://stc-tech@blobcdhlaunchpadda72dv.dfs.core.windows.net/dataset_list/', True)
dbutils.fs.rm('abfss://stc-tech@blobcdhlaunchpadda72dv.dfs.core.windows.net/report_list/', True)

# COMMAND ----------

dbutils.fs.ls("abfss://stc-tech@blobcdhlaunchpadda72dv.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table hive_metastore.stc_tech.workspace_list
# MAGIC (workspaceId string, workspaceName string)
# MAGIC LOCATION 'abfss://stc-tech@blobcdhlaunchpadda72dv.dfs.core.windows.net/workspace_list';

# COMMAND ----------

# %sql
# select * from hive_metastore.stc_tech.workspace_list

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table hive_metastore.stc_tech.dataset_list
# MAGIC (createdDate string, datasetId string, datasetName string, webUrl string, workspaceId string)
# MAGIC LOCATION 'abfss://stc-tech@blobcdhlaunchpadda72dv.dfs.core.windows.net/dataset_list';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.stc_tech.dataset_list

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table hive_metastore.stc_tech.report_list
# MAGIC (reportId string, reportName string, webUrl string, datasetId string, workspaceId string)
# MAGIC LOCATION 'abfss://stc-tech@blobcdhlaunchpadda72dv.dfs.core.windows.net/report_list';

# COMMAND ----------

dbutils.fs.ls("abfss://stc-tech@blobcdhlaunchpadda72dv.dfs.core.windows.net/")
