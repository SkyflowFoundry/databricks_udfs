# Tokenize from CSV

This workbook is a sample Databricks UDF that reads data from a CSV file, inserts it into a Skyflow vault, and stores the associated token to Databricks. You can use it as a reference to build your own UDFs.

## Before you start

Before you start, you need the following:

* A Databricks volume you can write to.
* Permissions to create a Databricks secret scope.
* A CSV file, with a data you want to tokenize, stored in your Databricks workspace.
* A Skyflow vault with a schema that is compatible with the CSV file. See [Create a vault](https://docs.skyflow.com/create-a-vault/). A Quickstart vault works for this example.
* A Skyflow service account's *credentials.json* file. The service account needs insert and tokenization permissions on the vault. See [Data governance overview](https://docs.skyflow.com/data-governance-overview/).

Additionally, gather the following information:

* Skyflow information:
  * You Skyflow account ID, vault ID, and vault URL. You can find these in Skyflow Studio.
  * The name of the table in your Skyflow vault where you want to insert the CSV data. This example uses the table name `persons`.
* Databricks information:
  * Your Databricks host, token, and [secret scope](https://docs.databricks.com/en/security/secrets/secret-scopes.html). You can find these in the Databricks UI.
  * The path to the CSV file in your Databricks workspace. This example uses the path is `/Volumes/main/default/test_volume/fake_data.csv`.

## Set up the UDF

1. Upload the *credentials.json* file to a Databricks volume. This example assumes the path is `/Volumes/main/default/test_volume/credentials.json`.
1. Create a JSON object with your Skyflow information as follows:

    ```json
    {
        "account_id": "$ACCOUNT_ID",
        "vault_url_prefix": "$VAULT_URL",
        "vault_id": "$VAULT_ID",
        "creds_path": "$CREDENTIALS_PATH"
    }
    ```

    For example, it might look something like this:

    ```json
    {
        "account_id": "a451b783713e4424a7d761bb7bbc84eb",
        "vault_url_prefix": "https://abfc8bee4242.vault.skyflowapis.com",
        "vault_id": "c35dcf755b60479cae67ae1362e49643",
        "creds_path": "/Volumes/main/default/test_volume/credentials.json"
    }
    ```

1.  Stringify the JSON object and upload it to a Databricks secret. You can do this using the Databricks REST API. For example:

    ```bash
    curl --request POST "https://${DATABRICKS_HOST}/api/2.0/secrets/put" \
    --header "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    --data '{
        "scope": ${DATABRICKS_SCOPE},
        "key": "${CONFIG_KEY_NAME}",
        "string_value": "{\"account_id\":\"$ACCOUNT_ID\",\"vault_url_prefix\":\"$VAULT_URL\",\"vault_id\":\"$VAULT_ID\",\"creds_path\":\"$CREDENTIALS_PATH\"}"
    }'
    ```

## Use the UDF

1. Create a map that maps the CSV columns to the Skyflow columns. For example, if `fake_data.csv` has the `customer_names`, `user_email_address`, and `social_security_number` columns, the map could be:

    | CSV column | Skyflow column |
    |------------|----------------|
    | customer_names | name |
    | user_email_address | email_address |
    | social_security_number | ssn |

1. Copy the contents of the `tokenize_from_csv.py` file into a Databricks notebook.
1. Update the `CONFIG_SECRET_KEY_NAME` and `DATABRICKS_SECRETS_SCOPE_NAME` with your secret information.
1. Click the `run` button on top of the notebook. This registers (or re-registers) the UDF.
1. Create a new cell. Select SQL as the language.
1. Call the `tokenizeCSV(csv_path, skyflow_table, column_map)` UDF, applying the column maps in CSV,SKYFLOW pairs. For example, `SELECT tokenizeCSV("/Volumes/main/default/testvol/fake_data.csv", "persons",  MAP("customer_names", "name", "user_email_address", "email_address", "social_security_number", "ssn")) AS detokenized_data;`