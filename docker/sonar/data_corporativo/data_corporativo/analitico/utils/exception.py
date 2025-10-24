# Databricks notebook source
import pyspark
def check_column_not_null(df: pyspark.sql.DataFrame, column: str):
    assert df.filter(f"isnull({column})").count() == 0, f"Column {column} contains null values"

def check_column_not_zero(df: pyspark.sql.DataFrame, column: str):
    assert df.filter(f"{column} == 0").count() == 0, f"Column {column} contains zero values"