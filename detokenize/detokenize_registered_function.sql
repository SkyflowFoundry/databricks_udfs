%sql
CREATE FUNCTION detokenize(token_count INTEGER, token_offset INTEGER, token_table STRING, token_columns ARRAY<STRING>)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  
  import numpy as np
  import pandas as pd
  import requests
  import sys

  from pyspark.sql.functions import col
  from pyspark.sql.functions import pandas_udf
  from pyspark.sql.types import StringType

  from skyflow.service_account import generate_bearer_token

  raw_names_df = _sqldf
  BATCH_SIZE = 25
  ACCOUNT_ID = <ACCOUNT_ID>
  DETOKENIZE_URL = <DETOKENIZE_URL>
  CREDS_FILE= <CREDS_FILE>
  BEARER_TOKEN, _ = generate_bearer_token(CREDS_FILE)

def get_tokens(token_count, token_offset, token_table, token_columns):
  --SELECT token columns FROM token table LIMIT token_count OFFSET token_offset
  --return appropriate values
  select_query_str = f"SELECT {','.join(token_columns)} FROM {token_table} LIMIT {token_count} OFFSET {token_offset}"
  tokens = spark.sql(select_query_str)
  return tokens

def detokenize_tokens(names) -> str:
  batched_names = [names[i : i + BATCH_SIZE] for i in range(0, len(names), BATCH_SIZE)]
  output = []
  for cur_batch in batched_names:
    detokenize_params = [{"token":cur_name, "redaction":"REDACTED"} for cur_name in cur_batch]
    print(f"detokenize_params={detokenize_params}")
    payload = {"detokenizationParameters":detokenize_params}
    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-SKYFLOW-ACCOUNT-ID': ACCOUNT_ID,
      'Authorization': f'Bearer {BEARER_TOKEN}'
    }
    try:
      resp = requests.post(DETOKENIZE_URL, json=payload, headers=headers)
    except Exception as e:
      raise e
    try:
      data = resp.json()
      for cur_record in data["records"]:
        output.append(cur_record["value"])
    except Exception as e:
      print(f"error parsing detokenize return {data}. Error = {e}")
      raise e
  return str(output)

return detokenize_tokens(get_tokens(token_count, token_offset, token_table, token_columns))
$$;