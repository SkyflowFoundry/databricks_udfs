## Tokenize from CSV

Tokenize from CSV is a custom Databricks workbook that enables users to insert and tokenize data to a Skyflow vault.

##### Prerequisites

Create a Skyflow vault with a schema that is compatible with the CSV input to be tokenized.

This workflow requires the following inputs to be passed when calling the UDF:

- **csv_file_path**: path to CSV data file
- **skyflow_table**: name of the table in Skyflow Vault to insert CSV data
- **csv_to_skyflow_cols**: A map from CSV column names to related Skyflow column names. Ex: `<"customer_names": "names", "user_email_address": "email", "phone_number": "phone_number">`

In addition to the input, the workflow requires a config secret to be added to a Databricks Secret Scope. The config should be valid JSON uploaded in the form of a string. You can use a JSON to string converter to accomplish this.

The config must contain:

- **creds_path**: path to the credentials.json for the service account you want to connect to the vault
- **vault_url_prefix**: the API URL up to ".com". Ex: https://abcdefg.vault.skyflowapis.com
- **account_id**: the Skyflow accountID associated with the vault
- **vault_id**: the ID for your vault

The following snippets show a sample config and how to upload it to Databricks secret scope.  

Raw JSON:
```JSON
{
    "creds_path": "/Volumes/main/default/testvol/databricks-tokenize-demo-creds.json",
    "vault_url_prefix": "https://abcdefg.vault.skyflowapis.com",
    "account_id": "account_id_value",
    "vault_id": "vault_id_value"
}
```

Upload Secret:
```bash
curl --request POST "https://${DATABRICKS_HOST}/api/2.0/secrets/put" \
--header "Authorization: Bearer ${DATABRICKS_TOKEN}" \
--data '{
    "scope": ${DATABRICKS_SCOPE},
    "key": "${CONFIG_KEY_NAME}",
    "string_value": "{\"creds_path\":\"/Volumes/main/default/testvol/databricks-tokenize-demo-creds.json\",\"vault_url_prefix\":\"https://abcdefg.vault.skyflowapis.com\", \"vault_id\":\"${VAULT_ID_VALUE}\",\"account_id\":\"${ACCOUNT_ID_VALUE}\"}"
}'
```

- `$DATABRICKS_HOST` is the URL for your databricks instance
- `$DATABRICKS_TOKEN` is a bearer token which can be obtained through your account settings in the Databricks UI
- `$DATABRICKS_SCOPE` is the scope for your databricks secrets (see https://docs.databricks.com/en/security/secrets/secret-scopes.html)
- `$CONFIG_KEY_NAME` is the name for the config secret

##### Using the UDF

1. Upload the target CSV file into a Databricks volume. Note the path to this file as `$INPUT_CSV`. You can obtain the full path of the file in Databricks by right-clicking on the file and selecting "copy file path".
2. Upload the configuration JSON to Databricks secret scope as explained above.
3. Copy this UDF into a Databricks notebook. Click the `run` button on top of the notebook. This registers (or re-registers) the UDF.
4. Create a new cell. Select SQL as the language. (You can also skip this step if you plan to use the function outside of Databricks UI).
5. Call the UDF. Ex: `SELECT tokenizeCSV("/Volumes/main/default/testvol/fake_data.csv", "table_for_csv",  MAP("name", "name", "email", "email")) AS detokenized_data;`