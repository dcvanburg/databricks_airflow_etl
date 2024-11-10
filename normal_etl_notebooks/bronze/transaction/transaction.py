# Databricks notebook source
import pyspark.sql.functions as psf
from random import randint, choice

def extract():
    # Sample transaction descriptions
    descriptions = [
        "Grocery Purchase", "Electronics Purchase", "Restaurant", "Clothing Store", 
        "Gas Station", "Online Shopping", "Utility Payment", "Healthcare", 
        "Subscription Service", "Travel Booking"
    ]

    # Generate multiple transactions for each ID with a description and amount
    data = [
        (
            id, 
            f"T{id}{transaction_num}",  # transaction_id
            choice(descriptions),       # description
            round(randint(5, 500) + randint(0, 99) / 100, 2)  # amount as float
        )
        for id in range(1, 21)
        for transaction_num in range(1, 6)  # Generate 5 transactions per id
    ]
    
    # Create DataFrame with id, transaction_id, description, and amount
    df = spark.createDataFrame(data, ["id", "transaction_id", "description", "amount"])
    
    return df

def write(df):
    df.write.mode('overwrite').saveAsTable('01_bronze.transaction')

# Run
df = extract()
write(df)
