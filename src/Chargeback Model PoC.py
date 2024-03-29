# Databricks notebook source
import requests

# Globals
databricks_workspace_url = "https://<ACCOUNT_NAME>.cloud.databricks.com/"
api_token = dbutils.secrets.get("dev", "api_token")
warehouses = [ "5badc23414", "42lkj2390190" ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.) Fetch query history

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW audit_logs_vw AS 
# MAGIC SELECT *
# MAGIC FROM json.`s3://bucket/audit_logs`

# COMMAND ----------

response = requests.get(
  f"{databricks_workspace_url}/api/2.0/sql/history/queries",
  headers={
    "Authorization": f"Bearer {api_token}"
  },
  json={
   "filter_by": {
     "warehouse_ids": warehouses
   }
  }
)
print(response.status_code)

queries_list = []
if response.status_code == 200:
  queries = response.json()['res']
  next_page = response.json()['has_next_page']
  if next_page:
    next_page_token = response.json()['next_page_token']

  # Parse warehouse queries
  for query in queries:
    parsed_query = [{
      'query_id': query['query_id'],
      'user_id': query['user_id'],
      'user_name': query['user_name'],
      'status': query['status'],
      'query_text': query['query_text'],
      'start_time_ms': query['query_start_time_ms'] if 'query_start_time_ms' in query else None,
      'end_time_ms': query['query_end_time_ms'] if 'query_end_time_ms' in query else None,
      'duration': query['duration'] if 'duration' in query else None
    }]
    queries_list += parsed_query 

if len(queries_list) > 0:
  df = spark.createDataFrame(queries_list, "query_id string, user_id long, user_name string, status string, query_text string, start_time_ms long, end_time_ms long, duration long")
  df.display()
  df.createOrReplaceTempView('warehouse_query_history')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC  FROM system.compute.warehouse_events
# MAGIC  WHERE warehouse_id = '5ab5dda58c1ea16b' AND
# MAGIC        to_date(event_time) = curdate()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   query_id,
# MAGIC   user_name,
# MAGIC   duration / 1000 AS seconds,
# MAGIC   start_time_ms,
# MAGIC   end_time_ms
# MAGIC   FROM warehouse_query_history
# MAGIC  WHERE duration IS NOT NULL
# MAGIC    AND status IN ('FINISHED', 'FAILED')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.) Retrieve the number of concurrent users

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW concurrent_warehouse_users AS
# MAGIC SELECT
# MAGIC   start_date,
# MAGIC   start_hour,
# MAGIC   start_min,
# MAGIC   count(distinct user_id) as num_concurrent_users
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC     to_date(from_unixtime(start_time_ms / 1000)) as start_date,
# MAGIC     from_unixtime(start_time_ms / 1000) as start_ts,
# MAGIC     from_unixtime(end_time_ms / 1000) as end_ts,
# MAGIC     hour(start_ts) as start_hour,
# MAGIC     minute(start_ts) as start_min
# MAGIC   FROM warehouse_query_history
# MAGIC )
# MAGIC GROUP BY start_date, start_hour, start_min
# MAGIC ORDER By start_date, start_hour, start_min;
# MAGIC SELECT * FROM concurrent_warehouse_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join HR table, query history, concurrent_users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   user_name, 
# MAGIC   to_date(from_unixtime(start_time_ms / 1000)) as start_date
# MAGIC   
# MAGIC FROM
# MAGIC   warehouse_query_history query_hist
# MAGIC   -- TODO: Join user_name with HR table
# MAGIC   --JOIN hr_table hr ON hr.user_name = query_hist.user_name
# MAGIC   JOIN 

# COMMAND ----------

warehouse_id = "5ab5dda58c1ea16b"
response = requests.get(
  f"{databricks_workspace_url}/api/2.0/sql/warehouses/{warehouse_id}",
  headers={
    "Authorization": f"Bearer {api_token}"
  }
)
print(response.status_code)
print(response.json())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO : UDF to get the number of concurrent users
# MAGIC -- TODO: slice the event_time into per minute
# MAGIC select * 
# MAGIC   from system.compute.warehouse_events
# MAGIC  where warehouse_id = '5ab5dda58c1ea16b'
# MAGIC

# COMMAND ----------


