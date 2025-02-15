# Databricks notebook source
# MAGIC %run "/Users/szymanski.ks@pg.com/Get_PBI_Objects/Get_PowerBI_Functions"

# COMMAND ----------

import json
import requests
import os

# COMMAND ----------

subscription_id = 'b8dec86d-12dc-46f8-8729-cd7fa3c1ed3c' # PG-NA-External-Spoke-DH-NonProd-04
key_vault_scope = "kv-cdh-launchpad-da-72"

client_id_az = dbutils.secrets.get(scope=key_vault_scope, key="az-sp-extended-devops-app-da-72dv-ID")
client_secret_az = dbutils.secrets.get(scope=key_vault_scope, key="az-sp-extended-devops-app-da-72dv-KEY")
tenant_id_az = dbutils.secrets.get(scope=key_vault_scope, key="tenant-id")

client_id = dbutils.secrets.get(scope="kv-cdh-launchpad-da-72", key="STC-PMRA-PBI-D-ID")
client_secret = dbutils.secrets.get(scope="kv-cdh-launchpad-da-72", key="STC-PMRA-PBI-D-Key")
tenant_id = dbutils.secrets.get(scope="kv-cdh-launchpad-da-72", key="tenant-id")

spark.conf.set("fs.azure.account.auth.type.blobcdhlaunchpadda72dv.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.blobcdhlaunchpadda72dv.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.blobcdhlaunchpadda72dv.dfs.core.windows.net", client_id_az)
spark.conf.set("fs.azure.account.oauth2.client.secret.blobcdhlaunchpadda72dv.dfs.core.windows.net", client_secret_az)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.blobcdhlaunchpadda72dv.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id_az}/oauth2/token")

# COMMAND ----------

# Service Principal Information

# client_id = dbutils.secrets.get(scope="kv-cdh-launchpad-da-72", key="STC-PMRA-PBI-D-ID")
# client_secret = dbutils.secrets.get(scope="kv-cdh-launchpad-da-72", key="STC-PMRA-PBI-D-Key")
# tenant_id = dbutils.secrets.get(scope="kv-cdh-launchpad-da-72", key="tenant-id")

base_url = f"https://api.powerbi.com/v1.0/myorg/"

jsonDatasetRS = []
jsonDatasetRefHist = []

# COMMAND ----------

# access_token = get_accessToken(client_id, client_secret, tenant_id)
access_token = get_accessToken(client_id, client_secret, tenant_id)
headers = {"Authorization": f"Bearer {access_token}"}
# display(headers)

# COMMAND ----------

# df_dts = spark.read.format("delta").load("hive_metastore/stc_tech/dataset_list")
df_dts = spark.sql("select datasetId, workspaceId from hive_metastore.stc_tech.dataset_list")
# display(df_dts)

dts_list = df_dts.collect()
display(dts_list)

# COMMAND ----------

workspace_id = '266bad0e-3e6f-4408-8f67-0e61cd59eebe'
dataset_id = 'ba932e7b-f310-4af3-bf05-dc5806bea64b'
# workspace_id = '005e4e7a-5cbe-404b-94fd-2618eb2aed45'
# dataset_id = 'a8a26d79-a2bc-43ed-a484-6677f358ca11'	
jsonDatasetRS.clear()

# if (len(dts_list) > 0):
#     for idx in range(len(dts_list)):
#         workspace_id = dts_list[idx]['workspaceId']
#         dataset_id = dts_list[idx]['datasetId']
#         # print(workspace_id)
#         # print(dataset_id)
        
#         new_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule"

#         #Set the GET response using the relative URL
#         response = get_pbiDataset_Refresh_Schedule(workspace_id, dataset_id, base_url, headers).json()
#         display(response)

        # days = ', '.join(response['days'])
        # times = '; '.join(response['times'])

        # data = {
        #     "datasetId": dataset_id
        #     , "days": days
        #     , "times": times
        # }

        # jsonData = json.dumps(data)
        # jsonDatasetRS.append(jsonData)
        # jsonRDD = sc.parallelize(jsonDatasetRS)


response = get_pbiDataset_Refresh_Schedule(workspace_id, dataset_id, base_url, headers).json()

# relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule"
# response = requests.post(relative_url, headers=headers).json()
# display(response)

days = ', '.join(response['days'])
times = '; '.join(response['times'])

# display(days)
# display(times)

data = {
    "datasetId": dataset_id
    , "days": days
    , "times": times
}

jsonData = json.dumps(data)
jsonDatasetRS.append(jsonData)
jsonRDD = sc.parallelize(jsonDatasetRS)

df_out = spark.read.json(jsonRDD)
display(df_out)


# COMMAND ----------

workspace_id = '266bad0e-3e6f-4408-8f67-0e61cd59eebe'
dataset_id = 'ba932e7b-f310-4af3-bf05-dc5806bea64b'
# workspace_id = '005e4e7a-5cbe-404b-94fd-2618eb2aed45'
# dataset_id = 'a8a26d79-a2bc-43ed-a484-6677f358ca11'	


jsonDatasetRefHist.clear()

relative_url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1'

response = requests.get(relative_url, headers=headers).json()
# display(response)

# startTime = response['value'][0]['startTime']
# endTime = response['value'][0]['endTime']
# refreshType = response['value'][0]['refreshType']
# status = response['value'][0]['status']

data = {
    'dataset_id' : dataset_id
    , 'startTime' : response['value'][0]['startTime']
    , 'endTime' : response['value'][0]['endTime']
    , 'refreshType' : response['value'][0]['refreshType']
    , 'status' : response['value'][0]['status']
}

jsonData = json.dumps(data)
jsonDatasetRefHist.append(jsonData)
jsonRDD = sc.parallelize(jsonDatasetRefHist)

df_refh = spark.read.json(jsonRDD)
display(df_refh)
