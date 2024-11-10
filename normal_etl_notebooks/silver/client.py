# Databricks notebook source
import pyspark.sql.functions as psf

def extract():

    df1 = spark.table('01_bronze.client_names')
    df2 = spark.table('01_bronze.client_address')
    
    return df1, df2
  
def transform(df1, df2):

    df = (
      df1.join(
        df2,
        how='left',
        on='id')
    )
    
    return df

def write(df):
    df.write.mode('overwrite').saveAsTable('02_silver.client')

# Run
df = extract()
df_write = transform(df)
write(df_write)
