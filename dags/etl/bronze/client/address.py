# Databricks notebook source
import pyspark.sql.functions as psf

def extract():
  # Create a DataFrame with id, address, and country columns
  df = spark.createDataFrame(
    [
      (1, "123 Maple St", "USA"),
      (2, "456 Oak St", "USA"),
      (3, "789 Pine St", "Canada"),
      (4, "101 Cedar St", "Canada"),
      (5, "202 Birch St", "UK"),
      (6, "303 Walnut St", "UK"),
      (7, "404 Ash St", "Germany"),
      (8, "505 Chestnut St", "Germany"),
      (9, "606 Elm St", "France"),
      (10, "707 Willow St", "France"),
      (11, "808 Poplar St", "Italy"),
      (12, "909 Magnolia St", "Italy"),
      (13, "1010 Dogwood St", "Spain"),
      (14, "1111 Cypress St", "Spain"),
      (15, "1212 Fir St", "Australia"),
      (16, "1313 Sycamore St", "Australia"),
      (17, "1414 Redwood St", "Netherlands"),
      (18, "1515 Sequoia St", "Netherlands"),
      (19, "1616 Spruce St", "Sweden"),
      (20, "1717 Maple St", "Sweden")
    ],
    ["id", "address", "country"]
  )
    
  return df

def write(df):
    df.write.mode('overwrite').saveAsTable('01_bronze.client_address')
