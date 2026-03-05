# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Style Assistant: Log & Deploy
# MAGIC Logs agent.py as MLflow model and deploys to Model Serving.

# COMMAND ----------

# MAGIC %pip install mlflow==3.6.0 databricks-langchain databricks-vectorsearch databricks-agents
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex,
)

CATALOG = "amitabh_arora_catalog"
SCHEMA = "tko27_retail"
LLM_ENDPOINT = "databricks-claude-opus-4-6"
VS_INDEX = f"{CATALOG}.{SCHEMA}.products_description_index"

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksVectorSearchIndex(index_name=VS_INDEX),
]

with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        name="style_assistant",
        python_model="agent.py",
        resources=resources,
        pip_requirements=[
            "mlflow==3.6.0",
            "databricks-langchain",
            "databricks-vectorsearch",
            "databricks-agents",
        ],
        input_example={
            "input": [{"role": "user", "content": "CUST-00001"}]
        },
        registered_model_name=f"{CATALOG}.{SCHEMA}.style_assistant",
    )
    print(f"Model logged: {model_info.model_uri}")

# COMMAND ----------

from databricks import agents

deployment = agents.deploy(
    f"{CATALOG}.{SCHEMA}.style_assistant",
    model_version=model_info.registered_model_version,
    endpoint_name="style-assistant-endpoint",
)
print(f"Deploying to: style-assistant-endpoint")
print("Deployment takes ~15 min. Check status in Serving UI.")
