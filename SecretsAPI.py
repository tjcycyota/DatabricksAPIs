# Databricks notebook source
# MAGIC %md ## Install Databricks CLI to access APIs

# COMMAND ----------

# MAGIC %pip install databricks_cli

# COMMAND ----------

# MAGIC %md ## Instantiate API client

# COMMAND ----------

from databricks_cli.sdk.api_client import ApiClient
apiclient = ApiClient(token = dbutils.entry_point.getDbutils().notebook().getContext().apiToken().get(),
                      host = dbutils.entry_point.getDbutils().notebook().getContext().apiUrl().get())

# COMMAND ----------

# MAGIC %md ## Create Secret Scope

# COMMAND ----------

apiclient.perform_query("POST","/secrets/scopes/create",
  {
  "scope": "my_secret_SCOPE",
  "initial_manage_principal": "users"
  })

# COMMAND ----------

# MAGIC %md ## Lists Secret Scopes to confirm created

# COMMAND ----------

apiclient.perform_query("GET", "/secrets/scopes/list")

# COMMAND ----------

# MAGIC %md ## Put secret in scope

# COMMAND ----------

apiclient.perform_query("POST","/secrets/put",
{
  "scope": "my_secret_SCOPE",
  "key": "my_secret_KEY",
  "string_value": "my_super_secret_token"
})

# COMMAND ----------

# MAGIC %md ## Retrieve secret using DBUTILS

# COMMAND ----------

token = dbutils.secrets.get(scope="my_secret_SCOPE", key="my_secret_KEY")
