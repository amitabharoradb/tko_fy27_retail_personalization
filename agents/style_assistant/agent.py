# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Style Assistant Agent
# MAGIC MLflow ResponsesAgent using Claude Opus 4.6 via FMAPI + Vector Search.

# COMMAND ----------

# MAGIC %pip install mlflow==3.6.0 databricks-langchain databricks-vectorsearch databricks-agents
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import mlflow
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex,
    DatabricksSQLWarehouse,
)
from databricks_langchain import ChatDatabricks
from databricks.vector_search.client import VectorSearchClient
from databricks import sql as dbsql
from typing import Generator
import os

# Config
LLM_ENDPOINT = "databricks-claude-opus-4-6"
VS_ENDPOINT = "tko27_vs_endpoint"
VS_INDEX = "amitabh_arora_catalog.tko27_retail.products_description_index"
CATALOG = "amitabh_arora_catalog"
SCHEMA = "tko27_retail"

SYSTEM_PROMPT = """You are a personal style assistant for a premium retail brand. You have access to the customer's browsing interests, purchase history, and a product catalog.

Your job:
1. Analyze the customer's current interests and past purchases
2. Recommend 5 products that match their style profile
3. Explain WHY each product fits their preferences

Be conversational, enthusiastic, and specific. Reference their actual browsing and purchase patterns.

Return your response as valid JSON with this structure:
{
  "recommended_products": [
    {"product_id": "...", "name": "...", "category": "...", "price": 0.0, "reasoning": "..."}
  ],
  "reasoning": "Overall summary of why these recommendations fit..."
}"""


class StyleAssistant(ResponsesAgent):
    def __init__(self):
        self.llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.3, max_tokens=2000)
        self.vsc = VectorSearchClient()

    def _get_interests(self, customer_id: str) -> list[dict]:
        """Fetch top 3 interests from Gold table."""
        df = spark.sql(f"""
            SELECT category, intent_score, rank
            FROM {CATALOG}.{SCHEMA}.customer_current_interests
            WHERE customer_id = '{customer_id}'
            ORDER BY rank
            LIMIT 3
        """)
        return [r.asDict() for r in df.collect()]

    def _get_purchases(self, customer_id: str) -> list[dict]:
        """Fetch last 5 purchases."""
        df = spark.sql(f"""
            SELECT ph.product_id, p.name, p.category, ph.total_amount, ph.purchase_date
            FROM {CATALOG}.{SCHEMA}.purchase_history ph
            JOIN {CATALOG}.{SCHEMA}.products p ON ph.product_id = p.product_id
            WHERE ph.customer_id = '{customer_id}'
            ORDER BY ph.purchase_date DESC
            LIMIT 5
        """)
        return [r.asDict() for r in df.collect()]

    def _search_products(self, query_text: str, num_results: int = 5) -> list[dict]:
        """Vector search for similar products."""
        idx = self.vsc.get_index(VS_ENDPOINT, VS_INDEX)
        results = idx.similarity_search(
            query_text=query_text,
            columns=["product_id", "name", "category", "price"],
            num_results=num_results,
        )
        rows = results.get("result", {}).get("data_array", [])
        cols = ["product_id", "name", "category", "price", "score"]
        return [dict(zip(cols, r)) for r in rows]

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        # Extract customer_id from user message
        user_msg = ""
        for m in request.input:
            if m.role == "user":
                user_msg = m.content
                break

        customer_id = user_msg.strip()

        # Gather context
        interests = self._get_interests(customer_id)
        purchases = self._get_purchases(customer_id)

        # Build VS query from interests
        interest_cats = [i["category"] for i in interests] if interests else ["Denim", "Tops"]
        vs_query = " ".join(interest_cats) + " style recommendations"
        similar_products = self._search_products(vs_query)

        # Build prompt
        context = f"""
Customer ID: {customer_id}

Current Interests (from browsing behavior):
{json.dumps(interests, indent=2, default=str)}

Recent Purchases:
{json.dumps(purchases, indent=2, default=str)}

Similar Products from Catalog (via semantic search):
{json.dumps(similar_products, indent=2, default=str)}
"""

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Generate personalized recommendations for this customer:\n{context}"},
        ]

        response = self.llm.invoke(messages)

        return ResponsesAgentResponse(
            output=[self.create_text_output_item(text=response.content, id="msg_1")]
        )

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        result = self.predict(request)
        for item in result.output:
            yield ResponsesAgentStreamEvent(type="response.output_item.done", item=item)


AGENT = StyleAssistant()
mlflow.models.set_model(AGENT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log & Deploy

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
