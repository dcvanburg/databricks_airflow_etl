# Databricks notebook source
import pyspark.sql.functions as psf

def extract():

  df1 = spark.table('02_silver.transaction')
  df2 = spark.table('02_silver.client')
    
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
  df.write.mode('overwrite').saveAsTable('03_gold.client_transaction')
