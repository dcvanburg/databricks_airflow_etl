# Databricks notebook source
import pyspark.sql.functions as psf

def extract():

    df = spark.table('01_bronze.transaction')
    
    return df
  
def transform(df):

    df = (
      df.withColumn('amount_int', (psf.col('amount') * 100).cast("int") )
    )
    
    return df

def write(df):
    df.write.mode('overwrite').saveAsTable('02_silver.transaction')

# Run
df = extract()
df_write = transform(df)
write(df_write)
