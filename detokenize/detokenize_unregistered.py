import pandas as pd
import requests

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

from skyflow.service_account import generate_bearer_token

raw_data_df = _sqldf
BATCH_SIZE = 25
ACCOUNT_ID = '<INSERT_ACCOUNT_ID_HERE>'
DETOKENIZE_URL = '<INSERT_DETOKENIZE_URL_HERE>'

#Should move to Kubernetes secrets for production env
CREDS_FILE='<INSERT_CREDS_FILE_PATH_HERE>'
BEARER_TOKEN, _ = generate_bearer_token(CREDS_FILE)

def detokenize(names: pd.Series) -> pd.Series:
  batched_names = [names[i : i + BATCH_SIZE] for i in range(0, len(names), BATCH_SIZE)]
  output = []
  for cur_batch in batched_names:
    detokenize_params = [{"token":cur_name,} for cur_name in cur_batch]
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
      print(resp.json())
    except Exception as e:
      raise e
    try:
      data = resp.json()
      for cur_record in data["records"]:
        output.append(cur_record["value"])
    except Exception as e:
      print(f"error parsing detokenize return {data}. Error = {e}")
      raise e
  return pd.Series(output)

df = raw_data_df
call_udf = pandas_udf(detokenize, returnType=StringType())
data_series = df.select("name","ssn", "email_address").rdd.flatMap(lambda x: x).collect()
custom_df = spark.createDataFrame([(data_series[i][0], data_series[i][1], data_series[i][2],) for i in range(len(data_series))], ["name", "email_address","ssn"])
custom_df = custom_df.repartition(3)
display(df.select(call_udf("name"), call_udf("email_address"), call_udf("ssn")))