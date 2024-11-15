# Databricks notebook source
import pyspark.sql.functions as psf

def extract():
  # create a dataframe with an id column and names
  df = spark.createDataFrame(
      [
          (1, "Alice", 23),
          (2, "Bob", 30),
          (3, "Cathy", 25),
          (4, "David", 40),
          (5, "Eve", 35),
          (6, "Frank", 29),
          (7, "Grace", 28),
          (8, "Hank", 33),
          (9, "Ivy", 22),
          (10, "Jack", 31),
          (11, "Karen", 27),
          (12, "Leo", 45),
          (13, "Mona", 34),
          (14, "Nina", 32),
          (15, "Oscar", 26),
          (16, "Paul", 50),
          (17, "Quincy", 24),
          (18, "Rachel", 37),
          (19, "Sam", 39),
          (20, "Tina", 21)
      ],
      ["id", "name", "age"]
  )

  return df

def write(df):
    df.write.mode('overwrite').saveAsTable('01_bronze.client_names')
