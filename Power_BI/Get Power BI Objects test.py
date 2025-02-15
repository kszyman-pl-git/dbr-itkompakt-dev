# Databricks notebook source
from datetime import datetime
from time import sleep
import json
import requests
import os
from pyspark.sql.functions import split



# COMMAND ----------

key_vault_scope = "kv-from-Azure"

service_id = dbutils.secrets.get(scope=key_vault_scope, key="service-ID")
service_key = dbutils.secrets.get(scope=key_vault_scope, key="service-KEY")
pg_tenant_id = dbutils.secrets.get(scope=key_vault_scope, key="tenant-id")

# COMMAND ----------

wrks_arr = {}
report_arr = []
jsonWorkspaceList = []
jsonDatasetList = []
jsonReportList = []


# COMMAND ----------

# Service Principal Information

client_id = dbutils.secrets.get(scope=key_vault_scope, key="client-id")
client_secret = dbutils.secrets.get(scope=key_vault_scope, key="client-secret")
tenant_id = dbutils.secrets.get(scope=key_vault_scope, key="tenant-id")

sa_username = dbutils.secrets.get(scope=key_vault_scope, key="SA-User")
sa_password = dbutils.secrets.get(scope=key_vault_scope, key="SA-Password")

base_url = f"https://api.powerbi.com/v1.0/myorg/"
base_url_app = "https://api.powerbi.com.rproxy.goskope.com/v1.0/myorg/"

# display(client_secret)

# COMMAND ----------

# Function to get Access Token using App ID and Client Secret
def get_accessToken(client_id, client_secret, tenant_id):
    # Set the Token URL for Azure AD Endpoint
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
 
    # Data Request for Endpoint
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": "https://analysis.windows.net/powerbi/api",
    }
 
    # Send POST request to obtain access token
    response = requests.post(token_url, data=data)
 
    if response.status_code == 200:
        token_data = response.json()
        return token_data.get("access_token")
    else:
        response.raise_for_status()

# COMMAND ----------

# Function to get Access Token using User credentials
def get_accessToken_User(username, password, client_id, client_secret, tenant_id):
    # Set the Token URL for Azure AD Endpoint
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    
        # Data Request for Endpoint
    data = {
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": sa_username,
        "password": sa_password, 
        "resource": "https://management.azure.com/"
    }

    # Send POST request to obtain access token
    response = requests.post(token_url, data=data)
    display(response.content)

    # if response.status_code == 200:
    #     token_data = response.json()

    # display(token_data.get("access_token"))


# COMMAND ----------

# access_token_user = get_accessToken_User(sa_username, sa_password, client_id, client_secret, tenant_id)
# headers_user = {"Authorization": f"Bearer {access_token_user}"}
# display(headers_user)

# COMMAND ----------

# Function to get Workspaces list
def get_pbiWorkspace_List(base_url, headers):
    relative_url = base_url + "groups"
     
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
    return response

# COMMAND ----------

#Function to get Datasets List
def get_pbiDataset_List(workspace_id, base_url, headers):
    relative_url = base_url + f"groups/{workspace_id}/datasets"
 
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
    return response

# COMMAND ----------

#Function to get Reports List
def get_pbiReports_List(workspace_id, base_url, headers):
    relative_url = base_url + f"groups/{workspace_id}/reports"
 
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
    return response

# COMMAND ----------

#Function to get Apps List
def get_pbiApps_List(base_url, headers):
    relative_url = base_url + "apps"
    # display(relative_url)
 
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
    return response

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

df = spark.read.json(jsonRDD)
df_workspaces = df.select("id", "name").withColumnRenamed("id", "workspaceId").withColumnRenamed("name", "workspaceName")
# display(df_workspaces)

for index, item in enumerate(workspace_ids):
    id = item["id"]
    name = item["name"]
    wrks_arr.update({id: name})


# COMMAND ----------

for i, (key, value) in enumerate(wrks_arr.items()):
    tmpWorkspaceId = key
    dataset = get_pbiDataset_List(tmpWorkspaceId, base_url, headers)
    dataset_list = dataset.json()
    dataset_value = dataset_list['value'][0]
    jsonData = json.dumps(dataset_value)
    jsonDatasetList.append(jsonData)
    jsonRDD = sc.parallelize(jsonDatasetList)

df_dts = spark.read.json(jsonRDD)
df_dts2 = df_dts.select("createdDate", "id", "name", "webUrl") \
    .withColumnRenamed("id", "datasetId") \
    .withColumnRenamed("name", "datasetName") \
    .filter("webUrl is not null")
split_col = split(df_dts2['webUrl'], '/')
df_dts3 = df_dts2.withColumn('workspaceId', split_col.getItem(4))
# display(df_dts3)


# COMMAND ----------

for i, (key, value) in enumerate(wrks_arr.items()):
    tmpWorkspaceId = key
    report = get_pbiReports_List(tmpWorkspaceId, base_url, headers)
    report_list = report.json()
    report_value = report_list['value'][0]
    jsonData = json.dumps(report_value)
    jsonDatasetList.append(jsonData)
    jsonRDD = sc.parallelize(jsonDatasetList)

df_rep = spark.read.json(jsonRDD)
df_rep2 = df_rep.select("createdDate", "id", "name", "webUrl", "datasetId") \
    .withColumnRenamed("id", "reportId") \
    .withColumnRenamed("name", "reportName") \
    .filter("webUrl is not null") \
    .filter(df_rep.reportType == 'PowerBIReport')
split_col = split(df_rep2['webUrl'], '/')
df_rep3 = df_rep2.withColumn('workspaceId', split_col.getItem(4))
# display(df_rep3)


# COMMAND ----------

# access_token_user = get_accessToken_User(sa_username, sa_password, client_id, client_secret, tenant_id)
# headers_user = {"Authorization": f"Bearer {access_token_user}"}
# display(headers_user)


# Get Apps List
# app = get_pbiApps_List(base_url_app, headers)
# # app_list = app.json()
# display(app.content)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.{storage-account}.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.{storage-account}..dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.{storage-account}..dfs.core.windows.net", service_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.{storage-account}..dfs.core.windows.net", service_key)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.{storage-account}..dfs.core.windows.net", f"https://login.microsoftonline.com/{pg_tenant_id}/oauth2/token")

df_workspaces.write.format("delta").mode("overwrite").saveAsTable("hive_metastore.db-schema.workspace_list")

df_dts3.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("hive_metastore.db-schema.dataset_list")

df_rep3.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("hive_metastore.db-schema.report_list")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.db-schema.report_list
