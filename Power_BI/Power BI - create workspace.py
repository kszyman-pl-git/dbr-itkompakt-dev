# Databricks notebook source
# MAGIC %run "/Users/szymanski.ks@pg.com/Get_PBI_Objects/Get_PowerBI_Functions"

# COMMAND ----------

import json
import requests
import os


# COMMAND ----------

# Service Principal Information

client_id = dbutils.secrets.get(scope="kv-from-Azure", key="client-id")
client_secret = dbutils.secrets.get(scope="kv-from-Azure", key="Ssecret-id")
tenant_id = dbutils.secrets.get(scope="kv-from-Azure", key="tenant-id")

w_id = ''
cap_id = ''

# COMMAND ----------

access_token = get_accessToken(client_id, client_secret, tenant_id)
headers = {"Authorization": f"Bearer {access_token}"}
# display(headers)

# COMMAND ----------

# Workspace deleting

# w_id_del = ''
# del_url = f"https://api.powerbi.com/v1.0/myorg/groups/{w_id_del}"
# response = requests.delete(del_url, headers=headers)
# display(response.json())

# COMMAND ----------

# Create a workspace

create_url = 'https://api.powerbi.com/v1.0/myorg/groups?workspaceV2=True'

data = {
    "name": "STCA_MUSE_DEV2"
}

response = requests.post(create_url, headers=headers, json=data)
work = response.json()
# display(work)

w_id = work['id']
# display(w_id)

# COMMAND ----------

cap_url = 'https://api.powerbi.com/v1.0/myorg/capacities'
response = requests.get(cap_url, headers=headers)
output = response.json()
cap_id = output['value'][0]['id']
display(cap_id)


# COMMAND ----------

# Assign workspace to Premium capacity
premium_url = f'https://api.powerbi.com/v1.0/myorg/groups/{w_id}/AssignToCapacity'

data = {
    "capacityId": cap_id
}

response = requests.post(premium_url, headers=headers, json=data)
display(response.status_code)


# COMMAND ----------

# Add permissions to user

# w_id = '941a9cff-eaaa-4f39-b324-0ce7b3299d2c'

perm_url = f"https://api.powerbi.com/v1.0/myorg/groups/{w_id}/users"

data = {
    "emailAddress": "full.name@domain.com"
    , "groupUserAccessRight": "Admin"
    , "principalType": "User"
}

response = requests.post(perm_url, headers=headers, json=data)
# display(response)

# COMMAND ----------

# Add prmissions to user

perm_url2 = f"https://api.powerbi.com/v1.0/myorg/groups/{w_id}/users"

data = {
    "emailAddress": "full.name@domain.com"
    , "groupUserAccessRight": "Admin"
    , "principalType": "User"
}

response = requests.post(perm_url2, headers=headers, json=data)
# display(response)

# COMMAND ----------

# Add prmissions to group

perm_url3 = f"https://api.powerbi.com/v1.0/myorg/groups/{w_id}/users"

data3 = {
    "identifier": "026078ad-87fc-4634-aced-e30b6bc56924" 
    , "groupUserAccessRight": "Viewer"
    , "principalType": "Group"
}

response = requests.post(perm_url3, headers=headers, json=data3)
# display(response.content)

# COMMAND ----------

# Add prmissions to group

perm_url4 = f"https://api.powerbi.com/v1.0/myorg/groups/{w_id}/users"

data4 = {
    "identifier": "9c8a1e3c-d67b-4996-bde8-8c42ebb5cd7d"
    , "groupUserAccessRight": "Admin"
    , "principalType": "Group"
}

response = requests.post(perm_url4, headers=headers, json=data4)
# display(response.content)

# COMMAND ----------

# graph_url = "https://graph.microsoft.com/v1.0/groups?$filter=displayName eq 'Azure-group'"

# response = requests.get(graph_url, headers=headers).json()
# display(response)

# COMMAND ----------

# url_get = f'https://api.powerbi.com/v1.0/myorg/groups/{w_id}/users'

# response = requests.get(url_get, headers=headers).json()
# display(response)

