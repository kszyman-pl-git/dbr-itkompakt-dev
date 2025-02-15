# Databricks notebook source
# MAGIC %md
# MAGIC #Refreshes a PowerBI dataset
# MAGIC ###Parameters:
# MAGIC * pbi_workspace - id of the PowerBI workspace
# MAGIC * pbi_dataset - id of the PowerBI dataset
# MAGIC * pbi_env - name of environment type : prod / test / dev, dev is default
# MAGIC * wait_for_refresh_end - indicates if script should wait until datset is refreshed : 0 - no / 1 - yes

# COMMAND ----------

# pbi_workspace =  dbutils.widgets.get("pbi_workspace")
# pbi_dataset =  dbutils.widgets.get("pbi_dataset")
# pbi_env =  dbutils.widgets.get("pbi_env")
# wait_for_refresh_end =  dbutils.widgets.get("wait_for_refresh_end")
pbi_workspace = "266bad0e-3e6f-4408-8f67-0e61cd59eebe"
pbi_dataset = "70a20a92-0000-4e42-b5f7-e9019dded33d"
pbi_env = "dev"
wait_for_refresh_end = "0"

# COMMAND ----------

from datetime import datetime
from time import sleep
import json
import requests

# COMMAND ----------

# Service Principal Information

client_id = dbutils.secrets.get(scope="kv-from-Azure", key="client-id")
client_secret = dbutils.secrets.get(scope="kv-from-Azure", key="client-secret-id")
tenant_id = dbutils.secrets.get(scope="kv-from-Azure", key="tenant-id")
    
base_url = f"https://api.powerbi.com/v1.0/myorg/"

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
    # display(response.content)
 
    if response.status_code == 200:
        token_data = response.json()
        return token_data.get("access_token")
    else:
        response.raise_for_status()

# COMMAND ----------

# Function to get workspace ID 
def get_pbiWorkspaceId(workspace_name, base_url, headers):
    relative_url = base_url + "groups"
     
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
     
    if response.status_code == 200:
        data = response.json()
        for workspace in data["value"]:
            if workspace["id"] == workspace_name:
                return workspace["id"]
        return None

# COMMAND ----------

#Function to get Dataset ID 
def get_pbiDatasetId(workspace_id, base_url, headers, dataset_name):
    relative_url = base_url + f"groups/{workspace_id}/datasets"
 
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
     
    if response.status_code == 200:
        dataset_id = []
        data = response.json()
        for dataset in data["value"]:
            if dataset["id"] == dataset_name and dataset["isRefreshable"] == True:
                dataset_id = dataset["id"]
                return dataset_id
            return None

# COMMAND ----------

# Function to Refresh PBI Dataset
def invoke_pbiRefreshDataset(workspace_id, dataset_id, base_url, headers):
    relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    response = requests.post(relative_url, headers=headers)
 
    if response.status_code == 202:
        print(f"Dataset {pbi_dataset} refresh has been triggered successfully.")
    else:
        print(f"Failed to trigger dataset {pbi_dataset} refresh.")
    return response.status_code,response

# COMMAND ----------

# Function to Refresh Single Table in PBI Dataset
def invoke_pbiRefreshTableInDataset(workspace_id, dataset_id, base_url, headers, body):
    relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    response = requests.post(relative_url, headers=headers, json=body)
 
    if response.status_code == 202:
        print(f"Dataset {pbi_dataset} refresh has been triggered successfully.")
    else:
        print(f"Failed to trigger dataset {pbi_dataset} refresh.")
    return response.status_code,response

# COMMAND ----------

def get_pbiRefreshStatus(workspace_id, dataset_id, base_url, headers, check_every_seconds=30):
    #top gets only the last refresh
    relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
 
    refresh_status = "started" 
    while refresh_status!="Completed" and refresh_status!="Failed": #check every x seconds if refresh completed
        response = requests.get(relative_url, headers=headers) #check status
        last_refresh = response.json()["value"][0]
        refresh_status=last_refresh["status"]
        print("status" + " - " + refresh_status+" "+str(datetime.now()))
        sleep(check_every_seconds)
    return last_refresh #return the last refresh json

# COMMAND ----------

# Azure authentication to get an access token for further actions
access_token = get_accessToken(client_id, client_secret, tenant_id)
headers = {"Authorization": f"Bearer {access_token}"}

# Check if given Workspace ID exists
workspace_id = get_pbiWorkspaceId(pbi_workspace, base_url, headers)

# Get Dataset ID
dataset_id = get_pbiDatasetId(workspace_id, base_url, headers, pbi_dataset)

web_body = {
        "type": "Full",    
        "objects": [
            {
                "table": "Table"
            }
        ]
    }


invoke_pbiRefreshTableInDataset(workspace_id, dataset_id, base_url, headers, web_body)


# Invoke Refresh

# if wait_for_refresh_end == "0":
#     invoke_pbiRefreshDataset(workspace_id, dataset_id, base_url, headers)
# else:
#     invoke_pbiRefreshDataset(workspace_id, dataset_id, base_url, headers)
#     refresh_status_code,refresh_response = invoke_pbiRefreshDataset(workspace_id, dataset_id, base_url, headers)
#     if refresh_status_code!=202: #failed to start
#         start_time = datetime.now()
#         end_time = datetime.now()
#         status = "Failed"
#         error = refresh_response.json()["error"]["message"]
#         print(f"refresh failed to start with error - {error}")
#     else: 
#         sleep(30)  #sometimes status does not retrun correctly right after refresh start
#         # Get Refresh Status
#         resp = get_pbiRefreshStatus(workspace_id, dataset_id, base_url, headers)

#         #write response json values to variables 
#         start_time =resp["startTime"]
#         end_time = resp["endTime"]
#         status = resp["status"]
#         error = ""
#         if status=="Failed":
#             error_json = json.loads(resp["refreshAttempts"][0]["serviceExceptionJson"])
#             error = error_json["errorDescription"].replace("'","")
#             print(f"refresh failed to complete with error - {error}")
#         else:
#             print(f"refresh completed successfully")
