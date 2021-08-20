# Databricks notebook source
from databricks_cli.sdk.api_client import ApiClient
apiclient = ApiClient(token = dbutils.entry_point.getDbutils().notebook().getContext().apiToken().get(),
                      host = dbutils.entry_point.getDbutils().notebook().getContext().apiUrl().get())

# COMMAND ----------

# MAGIC %md ## Get all Pipelines

# COMMAND ----------

#Get all pipelines
pipelines = apiclient.perform_query("GET", "/pipelines")

# COMMAND ----------

#Print results in readable form
[str(i['pipeline_id'])+" -- "+str(i['name']) for i in pipelines['statuses']]

# COMMAND ----------

# MAGIC %md ## Get permissions on one Pipeline by Id

# COMMAND ----------

#Specify a pipeline by ID
pipelineid = '95837c99-1a86-4423-85f6-33ae3dd2a3f1'
apiclient.perform_query("GET", "/permissions/pipelines/"+ pipelineid)

# COMMAND ----------

# MAGIC %md ## Give a user/group view permissions

# COMMAND ----------

apiclient.perform_query("PATCH","/permissions/pipelines/"+ pipelineid,{
  "access_control_list":
        {"group_name": "DLT_View_Group",
         "permission_level": "CAN_VIEW"
        }
    })

# COMMAND ----------

#Specify a pipeline by ID
pipelineid = '95837c99-1a86-4423-85f6-33ae3dd2a3f1'
apiclient.perform_query("GET", "/permissions/pipelines/"+ pipelineid)

# COMMAND ----------


