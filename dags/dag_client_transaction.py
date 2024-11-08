# Databricks notebook source
from etl.bronze.client.names import extract

# COMMAND ----------

from datetime import datetime, timedelta
#from airflow import DAG
#from airflow.operators.python import PythonOperator

# Import bronze ETL functions
from etl.bronze.client.names import extract as bronze_client_name_extract, write as bronze_client_name_write
from etl.bronze.client.address import extract as bronze_client_address_extract, write as bronze_client_address_write
from etl.bronze.transaction.transaction import extract as bronze_transaction_extract, write as bronze_transaction_write

# Import silver ETL functions
from etl.silver.client import extract as silver_client_extract, transform as silver_client_transform, write as silver_client_write
from etl.silver.transaction import extract as silver_transaction_extract, transform as silver_transaction_transform, write as silver_transaction_write

# Import gold ETL functions
from etl.gold.client_transaction import extract as gold_client_transaction_extract, transform as gold_client_transaction_transform, write as gold_client_transaction_write


# run bronze
df = bronze_client_name_extract()
bronze_client_name_write(df)

df = bronze_client_address_extract()
bronze_client_address_write(df)

df = bronze_transaction_extract()
bronze_transaction_write(df)

# run silver
df1, df2 = silver_client_extract()
df = silver_client_transform(df1, df2)
silver_client_write(df)

df = silver_transaction_extract()
df = silver_transaction_transform(df)
silver_transaction_write(df)

# run gold
df = gold_client_transaction_extract()
df = gold_client_transaction_transform(df)
gold_client_transaction_write(df)
