# Databricks notebook source
# MAGIC %run "/Users/szymanski.ks@pg.com/Get_PBI_Objects/Get_PowerBI_Functions"

# COMMAND ----------

from datetime import datetime
from time import sleep
import json
import requests
import os
from pyspark.sql.functions import split



# COMMAND ----------

wrks_arr = {}
jsonWorkspaceList = []
jsonDatasetList = []
jsonReportList = []


# COMMAND ----------

# Service Principal Information
key_vault_scope = "kv-from-Azure"

client_id = dbutils.secrets.get(scope=key_vault_scope, key="STC-MUSE-PBI-D-ID")
client_secret = dbutils.secrets.get(scope=key_vault_scope, key="STC-MUSE-PBI-D-Key")
tenant_id = dbutils.secrets.get(scope=key_vault_scope, key="tenant-id")

base_url = f"https://api.powerbi.com/v1.0/myorg/"
base_url_app = "https://api.powerbi.com.rproxy.goskope.com/v1.0/myorg/"

# display(client_secret)

# COMMAND ----------

access_token = get_accessToken(client_id, client_secret, tenant_id)
headers = {"Authorization": f"Bearer {access_token}"}
# display(headers)

# COMMAND ----------

# Get Workspaces List
workspace = get_pbiWorkspace_List(base_url, headers)
workspace_list = workspace.json()
workspace_ids = workspace_list["value"]
jsonData = json.dumps(workspace_ids)
jsonDatasetList.append(jsonData)
jsonRDD = sc.parallelize(jsonDatasetList)

# display(workspace_list)

df = spark.read.json(jsonRDD)
df_workspaces = df.select("id", "name").withColumnRenamed("id", "workspaceId").withColumnRenamed("name", "workspaceName")
# df_workspaces2 = df_workspaces.withColumn("webUrl", lit("https://app.powerbi.com/groups/"))
display(df_workspaces)

for index, item in enumerate(workspace_ids):
    id = item["id"]
    name = item["name"]
    wrks_arr.update({id: name})


# COMMAND ----------

# Get Datasets List
jsonDatasetList.clear()

for i, (key, value) in enumerate(wrks_arr.items()):
    tmpWorkspaceId = key
    dataset = get_pbiDataset_List(tmpWorkspaceId, base_url, headers)
    dataset_list = dataset.json()

    if (len(dataset_list) > 0):
        for idx in range(len(dataset_list['value'])):
            dataset_value = dataset_list['value'][idx]
            jsonData = json.dumps(dataset_value)
            jsonDatasetList.append(jsonData)
            jsonRDD = sc.parallelize(jsonDatasetList)


df_dts = spark.read.json(jsonRDD)
# display(df_dts)
df_dts2 = df_dts.select("createdDate", "id", "name", "webUrl") \
    .withColumnRenamed("id", "datasetId") \
    .withColumnRenamed("name", "datasetName") \
    .filter("webUrl is not null")
split_col = split(df_dts2['webUrl'], '/')
df_dts3 = df_dts2.withColumn('workspaceId', split_col.getItem(4))
# df_dts4 = df_dts3.filter(df_dts3.workspaceId == '266bad0e-3e6f-4408-8f67-0e61cd59eebe')
display(df_dts3)


# COMMAND ----------

# Get Reports List
jsonReportList.clear()

for i, (key, value) in enumerate(wrks_arr.items()):
    tmpWorkspaceId = key
    report = get_pbiReports_List(tmpWorkspaceId, base_url, headers)
    report_list = report.json()

    # display(report_list)
    # display(len(report_list['value']))

    if (len(report_list) > 0):
        for idx in range(len(report_list['value'])):
            report_value = report_list['value'][idx]
            jsonData = json.dumps(report_value)
            jsonReportList.append(jsonData)
            jsonRDD = sc.parallelize(jsonReportList)

df_rep = spark.read.json(jsonRDD)
# display(df_rep)

df_rep2 = df_rep.select("id", "name", "webUrl", "datasetId") \
    .withColumnRenamed("id", "reportId") \
    .withColumnRenamed("name", "reportName") \
    .filter(df_rep.reportType == 'PowerBIReport')
split_col = split(df_rep2['webUrl'], '/')
df_rep3 = df_rep2.withColumn('workspaceId', split_col.getItem(4))
# df_rep4 = df_rep3.filter(df_rep3.workspaceId == '266bad0e-3e6f-4408-8f67-0e61cd59eebe')
display(df_rep3)


# COMMAND ----------

workspade_id = '3237c848-e577-4483-97dd-24cbf16f2be8'
report_1 = get_pbiReports_List(workspade_id, base_url, headers).json()

display(report_1)
