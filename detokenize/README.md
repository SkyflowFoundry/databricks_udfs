# Detokenize UDF's

This directory contains sample UDF's that take in a token or list of tokens, call Skyflow vault's detokenize endpoint, and return data in redacted, masked, or plain-text form.

## Before you start

Before you start, you need the following:

* A Skyflow vault that is populated with data. See [Tokenize from CSV](https://github.com/SkyflowFoundry/databricks_udfs/tree/main/tokenize_from_csv).
* A Skyflow service account's *credentials.json* file. The service account needs insert and tokenization permissions on the vault. See [Data governance overview](https://docs.skyflow.com/data-governance-overview/).

Additionally, gather the following information:

* Skyflow information:
  * You Skyflow account ID, vault ID, and vault URL. You can find these in Skyflow Studio.
  * The name of the table in your Skyflow vault where you want to insert the CSV data. This example uses the table name `persons`.

## Set up the UDF

1. Upload the *credentials.json* file to a Databricks volume. This example assumes the path is `/Volumes/main/default/test_volume/credentials.json`.

## Use the UDF

There are two versions of detokenize UDF's: registered and non-registered.

#### Registered UDF

The registered UDF can be utilized by making a single call to the registered detokenize function. The following steps outline the process. 

1. Edit detokenize_registered_function.sql to include all of the applicable values for your Skyflow vault.
1. Copy the SQL into a Databricks notebook. Run the workbook to register the UDF.
1. Utilize the UDF by calling the registered function with desired paramaters.

#### Non-Registered UDF

The non-registered UDF can be utilized by creating a workbook that includes an SQL call to retrieve the rows of data you want to detokenize, followed by the detokenize python script. Databricks will pass the results of the SQL query to the detokenize flow via a shared variable "_sqldf". The following steps outline the process.

1. In a new Databricks workbook, create an SQL cell. This cell should contain a query for all of the tokens you want to detokenize. EX: `SELECT monotonically_increasing_id() seqno, name FROM `main`.`default`.`customers` LIMIT 3;`
1. In the same workbook, create a new python cell. Add your non-registered detokenize code to this cell.
1. Run the workbook. This will trigger the cells to run consecutively, with the results of the SQL cell being passed to the detokenize cell. 