# Databricks notebook source
subscriptionId = 'subscription-id-from-Azure'

api_version = '2023-11-01'

# url = f'https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Consumption/usageDetails?api-version=2024-06-11&$expand=properties/additionalProperties'
url = f'https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Consumption/usageDetails?api-version={api_version}'
display(url)

# COMMAND ----------

import requests
import json

# COMMAND ----------

# Service Principal Information

key_vault = 'kv-value-from-Azure'

client_id = dbutils.secrets.get(scope=key_vault, key="client-id")
client_secret = dbutils.secrets.get(scope=key_vault, key="client-secret)
tenant_id = dbutils.secrets.get(scope=key_vault, key="tenant-id")


base_url = f"https://api.powerbi.com/v1.0/myorg/"

# display(tenant_id)

# COMMAND ----------

token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

# Data Request for Endpoint
data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "resource": "https://management.azure.com/",
}

# COMMAND ----------

response = requests.post(token_url, data=data)

access_token = response.json().get("access_token")
# display(access_token)

headers_resources = {"Authorization": f"Bearer {access_token}"}

# COMMAND ----------

response = requests.post(url, headers=headers_resources)
#response = requests.get(url)
display(response.content)
