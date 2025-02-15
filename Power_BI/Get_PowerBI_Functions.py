# Databricks notebook source
# Service Principal Information

key_vault = "kv-from-Azure"

client_id = dbutils.secrets.get(scope=key_vault, key="client-id")
client_secret = dbutils.secrets.get(scope=key_vault, key="STC-client-secret")
tenant_id = dbutils.secrets.get(scope=key_vault, key="tenant-id")

# sa_username = dbutils.secrets.get(scope=key_vault, key="SA-User")
# sa_password = dbutils.secrets.get(scope=key_vault, key="SA-Password")

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
    # display(response.content)

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

#Function to get Datasets Refresh Schedule
def get_pbiDataset_Refresh_Schedule(workspace_id, dataset_id, base_url, headers):
    relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule"
 
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
    return response
