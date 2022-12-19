# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <img src = 'https://raw.githubusercontent.com/Evogelpohl/linkArtifacts/main/dv_meet_dbx.png' />

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Set up
# MAGIC 
# MAGIC #### Create the Application, Client ID and Secret:
# MAGIC *  Follow the directions in this tutorial to create an application registration for use in connecting to PowerApps Dataverse. Tutorial here: https://learn.microsoft.com/en-us/power-apps/developer/data-platform/walkthrough-register-app-azure-active-directory
# MAGIC 
# MAGIC #### Apply permissions to Dataverse for your Client ID (App):
# MAGIC * With the Application Client ID you just created, following this link to apply permissions to your PowerApps instance's Dataverse dataset. https://learn.microsoft.com/en-us/power-platform/admin/manage-application-users. In my case, I applied "System Administrator" as I'm determining the least-privledge permission set, TODO.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importers

# COMMAND ----------

import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set variables necessary to query the Dataverse endpoint

# COMMAND ----------

# Retrieve secrets from our key vault.

my_dv_orgName  = dbutils.secrets.get(scope="demo-kv-scope",key="eric-demo-dataverse-org") 
my_dv_clientId = dbutils.secrets.get(scope="demo-kv-scope",key="eric-demo-dataverse-clientid") 
my_dv_secret   = dbutils.secrets.get(scope="demo-kv-scope",key="eric-demo-dataverse-secret") 
my_ad_tenant   = dbutils.secrets.get(scope="demo-kv-scope",key="bp-tenant-id") 

my_dv_entity   = 'contacts' #Type any endpoint entity name for export. TODO: Make dynamic with Databricks widget

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Get the API access token for the application (clientid)

# COMMAND ----------

def get_access_token(tenant_id, client_id, client_secret):
    # Set the token endpoint URL and the request payload
    token_endpoint_url = (
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    )
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": f"https://{my_dv_orgName}.crm.dynamics.com/.default",  # IMPORTANT. The proper key is dependant upon this scope matching your dataverse url/.default
    }

    # Make the POST request to the token endpoint
    response = requests.post(token_endpoint_url, data=payload)

    # Extract the access token from the response
    response_json = response.json()
    access_token = response_json["access_token"]

    return access_token


my_dv_accessToken = get_access_token(
    tenant_id=my_ad_tenant, client_id=my_dv_clientId, client_secret=my_dv_secret
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Function to retrive all records for each Dataverse entity (table) endpoint passed

# COMMAND ----------

def call_dataverse_endpoint(endpoint):
    # Empty list
    data = []

    # Headers
    headers = {
        "Authorization": f"Bearer {my_dv_accessToken}",
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8"
    }

    # Initial request
    response = requests.get(endpoint, headers=headers)

    # Loop through the responses until odata.nextLink is gone.
    while "@odata.nextLink" in response.json():
        # Append the data returned by the endpoint to the list
        data.extend(response.json()["value"])

        # Request the odata.nextLink URL
        response = requests.get(response.json()["@odata.nextLink"], headers=headers)

    # Append nextLink response data
    data.extend(response.json()["value"])

    # Return
    return data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Call the Dataverse function (Test with the `accounts` or `contacts` tables)
# MAGIC 
# MAGIC * The call_dataverse_endpoint function returns a dictionary object, so we'll need to conver it to a dataframe with a schema later. 
# MAGIC * You can easily see the endpoint convention for tables in your Dataverse org. Change `/contacts` with `/accounts` to switch. TODO: Query the table manifest and loop through tables of interest programmatically. That url is `https://your-org.crm.dynamics.com/api/data/v9.2/EntityDefinitions`

# COMMAND ----------

list_data = call_dataverse_endpoint(f'https://{my_dv_orgName}.crm.dynamics.com/api/data/v9.2/{my_dv_entity}')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a schema necessary to convert our dict object (`list_data`) to json, dataframe.
# MAGIC * Dynamically create the schema based off the function response (`list_data`). The following function is provided by PawaritL on Github and I found it useful in creating the schema for json objects where you'd rather not hand-crank the definition yourself. Github gist: https://gist.github.com/PawaritL/a9d50a7b80f93013bd739255dd206a70
# MAGIC 
# MAGIC * `Beware!` While dynamic schema inferring functions are great, using them in production scenarios may have unintended consequences. It's best practice to define the schema needed and evolve the resulting Databricks Delta tables using its schema-evolution feature sets (e.g. Delta Live Tables)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

import json
from typing import Optional, List, Dict

TYPE_MAPPER = {
  bool: BooleanType(),
  str: StringType(), 
  int: LongType(), 
  float: DoubleType()
}

def generate_schema(input_json: Dict, 
                    max_level: Optional[int] = None, 
                    stringify_fields: Optional[List[str]] = None, 
                    skip_fields: Optional[List[str]] = None
                   ) -> StructType:
  """
  User-friendly version for _populate_struct.
  Given an input JSON (as a Python dictionary), returns the corresponding PySpark schema
  
  :param input_json: example of the input JSON data (represented as a Python dictionary)
  :param max_level: maximum levels of nested JSON to parse, beyond which values will be cast as strings
  :param stringify_fields: list of fields to be directly cast as strings
  :param skip_fields: list of field names to completely ignore parsing and omit from the schema
  
  :return: pyspark.sql.types.StructType
  """
  level = 1
  return _populate_struct(input_json, level, max_level, stringify_fields, skip_fields)
  

def _populate_struct(input_json: Dict, 
                     level: int = 1, 
                     max_level: Optional[int] = None, 
                     stringify_fields: Optional[List[str]] = None, 
                     skip_fields: Optional[List[str]] = None
                     ) -> StructType:
  """
  Given an input JSON (as a Python dictionary), returns the corresponding PySpark StructType
  
  :param input_json: example of the input JSON data (represented as a Python dictionary)
  :param level: current level within the (nested) JSON. level=1 corresponds to the top level
  :param max_level: maximum levels of nested JSON to parse, beyond which values will be cast as strings
  :param stringify_fields: list of field names to be directly cast as strings
  :param skip_fields: list of field names to completely ignore parsing and omit from the schema
  
  :return: pyspark.sql.types.StructType
  """
  
  if not isinstance(input_json, dict):
    raise ValueError("invalid input JSON")
  if not (isinstance(level, int) and (level > 0)):
    raise ValueError("level must be greater than zero")
  if max_level and not (isinstance(max_level, int) and (max_level >= level)):
    raise ValueError("max_level must be greater than or equal to level (by default, level = 1)")
    
  filled_struct = StructType()
  nullable = True

  for key in input_json.keys():
    if skip_fields and (key in skip_fields):
      continue
    elif (stringify_fields and (key in stringify_fields)) or (max_level and (level >= max_level)):
      filled_struct.add(StructField(key, StringType(), nullable))
    elif isinstance(input_json[key], dict):
      inner_level = level + 1
      inner_struct = _populate_struct(input_json[key], inner_level, max_level, stringify_fields)
      inner_field = StructField(key, inner_struct, nullable)
      filled_struct.add(inner_field)
    elif isinstance(input_json[key], list):
      inner_level = level + 1
      inner_array = _populate_array(input_json[key], inner_level)
      inner_field = StructField(key, inner_array, nullable)
      filled_struct.add(inner_field)
    elif input_json[key] is not None:
      inner_type =  TYPE_MAPPER[type(input_json[key])]
      inner_field = StructField(key, inner_type, nullable)
      filled_struct.add(inner_field)
      
  return filled_struct

def _populate_array(input_array: List,  
                   level: int = 1
                  ):
  """
  Given an input Python list, returns the corresponding PySpark ArrayType
  :param input_array: input array data (represented as a Python list)
  :param level: current level within the (nested) JSON
  
  :return: pyspark.sql.types.ArrayType
  """
  
  if not isinstance(input_array, list):
    raise ValueError("Invalid input array")
  if not (isinstance(level, int) and (level > 0)):
    raise ValueError("level must be greater than zero")

  if len(input_array):
    head = input_array[0]
    inner_level = level + 1
    if isinstance(head, list):
      inner_array = _populate_array(head, inner_level)
      filled_array = ArrayType(inner_array)
    elif isinstance(head, dict):
      inner_struct = _populate_struct(head, inner_level)
      filled_array = ArrayType(inner_struct)
    else:
      inner_type = TYPE_MAPPER[type(head)]
      filled_array = ArrayType(inner_type)
  else:
    default_type = StringType()
    filled_array = ArrayType(default_type)
      
  return filled_array

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Use the Dynamic Schema Generator function on our `list_data` object and convert it a normal Databricks dataframe

# COMMAND ----------

# Auto create the schema as a StructType object type
data_schema = generate_schema(input_json=list_data[0])

# View the schema
# display(data_schema)

# Create a dataframe from our Dataverse API calls using the schema we just created
df = spark.createDataFrame(data=list_data, schema=data_schema)

# Count the records in our dataframe
df.count()

# View the dataframe
#display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Finally, write our data to a Delta table for all to consume

# COMMAND ----------

# Switch to the Unity catalog used for my demos
sql('USE CATALOG demos;')  

# Create a Dataverse database/schema
sql('CREATE DATABASE IF NOT EXISTS Dataverse;')

# Create our delta table from our dataframe
df.write.mode('overwrite').format('delta').saveAsTable(f'Dataverse.{my_dv_entity}')
