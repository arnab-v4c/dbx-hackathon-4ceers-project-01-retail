# Databricks notebook source
# DBTITLE 1,Test 1: LLM via dbutils auth (current approach in silver.py)
import json

print("=" * 60)
print("TEST 1: OpenAI client with dbutils auth")
print("=" * 60)

try:
    from openai import OpenAI
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_token = context.apiToken().get()
    api_url = context.apiUrl().get()
    print(f"  API URL: {api_url}")
    print(f"  Token obtained: {'Yes' if api_token else 'No'} (length={len(api_token) if api_token else 0})")

    client = OpenAI(
        api_key=api_token,
        base_url=f"{api_url}/serving-endpoints"
    )
    response = client.chat.completions.create(
        model="databricks-gpt-oss-20b",
        messages=[{"role": "user", "content": "Reply with only: HELLO_LLM_WORKS"}],
        temperature=0.0,
    )
    result = response.choices[0].message.content.strip()
    print(f"  LLM Response: {result}")
    print("  STATUS: PASS")
except Exception as e:
    print(f"  ERROR: {type(e).__name__}: {str(e)}")
    print("  STATUS: FAIL")

# COMMAND ----------

# DBTITLE 1,Test 2: LLM via mlflow.deployments (alternative approach)
print("=" * 60)
print("TEST 2: mlflow.deployments client")
print("=" * 60)

try:
    import mlflow.deployments
    client = mlflow.deployments.get_deploy_client("databricks")
    response = client.predict(
        endpoint="databricks-gpt-oss-20b",
        inputs={"messages": [{"role": "user", "content": "Reply with only: HELLO_MLFLOW_WORKS"}]}
    )
    result = response["choices"][0]["message"]["content"].strip()
    print(f"  LLM Response: {result}")
    print("  STATUS: PASS")
except Exception as e:
    print(f"  ERROR: {type(e).__name__}: {str(e)}")
    print("  STATUS: FAIL")

# COMMAND ----------

# DBTITLE 1,Test 3: Foundation Model API via ai_query SQL (SDP-safe)
print("=" * 60)
print("TEST 3: ai_query SQL function")
print("=" * 60)

try:
    df = spark.sql("""
        SELECT ai_query('databricks-gpt-oss-20b', 'Reply with only: HELLO_AI_QUERY_WORKS') as response
    """)
    result = df.collect()[0]["response"]
    print(f"  LLM Response: {result}")
    print("  STATUS: PASS")
except Exception as e:
    print(f"  ERROR: {type(e).__name__}: {str(e)}")
    print("  STATUS: FAIL")

# COMMAND ----------

# DBTITLE 1,Test 4: Requests with workspace token (SDP-safe alternative)
print("=" * 60)
print("TEST 4: requests + spark.conf token")
print("=" * 60)

try:
    import requests
    # In SDP, you can get the token from spark conf
    token = spark.conf.get("spark.databricks.token", None)
    host = spark.conf.get("spark.databricks.workspaceUrl", None)
    print(f"  spark.databricks.token available: {token is not None}")
    print(f"  spark.databricks.workspaceUrl: {host}")

    if token and host:
        resp = requests.post(
            f"https://{host}/serving-endpoints/databricks-gpt-oss-20b/invocations",
            headers={"Authorization": f"Bearer {token}"},
            json={"messages": [{"role": "user", "content": "Reply with only: HELLO_REQUESTS_WORKS"}]}
        )
        print(f"  HTTP status: {resp.status_code}")
        if resp.ok:
            result = resp.json()["choices"][0]["message"]["content"].strip()
            print(f"  LLM Response: {result}")
            print("  STATUS: PASS")
        else:
            print(f"  Response: {resp.text[:200]}")
            print("  STATUS: FAIL")
    else:
        print("  STATUS: SKIP (no token/host in spark.conf)")
except Exception as e:
    print(f"  ERROR: {type(e).__name__}: {str(e)}")
    print("  STATUS: FAIL")
