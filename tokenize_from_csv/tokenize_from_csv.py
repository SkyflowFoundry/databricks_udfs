import json
import os
import pandas as pd

from skyflow.errors import SkyflowError
from skyflow.service_account import generate_bearer_token, is_expired
from skyflow.vault import Client, InsertOptions, Configuration


BATCH_SIZE = 25
CONFIG_SECRET_KEY_NAME = "tokenize-demo-config"
DATABRICKS_SECRETS_SCOPE_NAME = "demoscope"
SKYFLOW_BEARER_TOKEN_NAME = "SKYFLOW_BEARER"

"""
Config Map is a secret stored within a databricks secret scope. The value should be valid json wrapped in a string.
Required JSON values:
- creds_path: a path to the service account creds.json
- vault_url_prefix: url prefix for the vault. EX: https://abcdefg.vault.skyflowapis.com
- account_id: account id associated with vault
- vault_id: the vault's id
"""
config_str = dbutils.secrets.get(scope=DATABRICKS_SECRETS_SCOPE_NAME, key=CONFIG_SECRET_KEY_NAME)
try:
    config_map = json.loads(config_str)
except Exception as e:
    print(f"An error occurred converting configuration {config_str} to json. Error: {e}")
    raise e

bearer_token = os.environ.get(SKYFLOW_BEARER_TOKEN_NAME)

def tokenProvider():
    global bearer_token
    if not bearer_token or is_expired(bearer_token):
        bearer_token, _ = generate_bearer_token(config_map["creds_path"])
        os.environ[SKYFLOW_BEARER_TOKEN_NAME] = bearer_token
    return bearer_token


def tokenize_csv(csv_file_path, skyflow_table, csv_to_skyflow_cols):
    csv_data = pd.read_csv(csv_file_path, usecols=list(csv_to_skyflow_cols.keys()))
    batched_data = [csv_data[i : i + BATCH_SIZE] for i in range(0, len(csv_data), BATCH_SIZE)]
    config = Configuration(config_map["vault_id"], config_map["vault_url_prefix"], tokenProvider)
    client = Client(config)
    output = []
    for cur_batch in batched_data:
        records_list = [{"table":skyflow_table, "fields": {csv_to_skyflow_cols[col_header]:cur_row[col_header] for col_header in csv_to_skyflow_cols.keys()}} for _,cur_row in cur_batch.iterrows()]
        try:
            options = InsertOptions(tokens=True)
            data = {"records": records_list}
            response = client.insert(data, options=options)
            cur_records = [response["records"][i]["fields"] for i in range(len(response["records"]))]
            output = [*output, *cur_records]
        except SkyflowError as e:
            raise e
        except Exception as e:
            raise e
    return str(output)
spark.udf.register("tokenizeCSV", tokenize_csv)