# Databricks notebook source
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType

# COMMAND ----------

col_names = ['col_name', 'col_1', 'col_3', 'col_3']
values = [
    ("a", 1, None, 1)
    , ("b", 2, 1, 2)
    , ("c", None, 2, 3)
    , ("d", 4, 3, None)
    , ("e", '', None, 'abcd')
]

df = spark.createDataFrame(data=values, schema=col_names)
display(df)

# COMMAND ----------

@udf(returnType=StringType())
def checkIfNull(strName, dfValue):
    res = None
    if dfValue is None or dfValue == '':
        res = f"column {strName} is null or empty"
        # res = strName
    return res



# COMMAND ----------

df2 = df.withColumn("col_1_check", checkIfNull(strName="col_1", dfValue=col("col_1")))
display(df2)

# COMMAND ----------

df.schema.fields[1].name
