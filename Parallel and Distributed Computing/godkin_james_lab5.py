# Databricks notebook source
#James Godkin

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType, StringType

txtFile = "dbfs:///FileStore/tables/baseballdatabank/Master.csv"
file = sc.textFile(txtFile)
#print(file.collect())
data = file.map(lambda data: (data.split(',')))
#print(data.collect())
dataNoHeaders = data.filter(lambda x: x != ['playerID', 'birthYear', 'birthMonth', 'birthDay', 'birthCountry', 'birthState', 'birthCity', 'deathYear', 'deathMonth', 'deathDay', 'deathCountry', 'deathState', 'deathCity', 'nameFirst', 'nameLast', 'nameGiven', 'weight', 'height', 'bats', 'throws', 'debut', 'finalGame', 'retroID', 'bbrefID'])
#print(dataNoHeaders.collect())
dataNoHeaders = dataNoHeaders.filter(lambda x: x[17] != '')
#print(dataNoHeaders.collect())
playerRows= dataNoHeaders.map(lambda L: Row(playerID=L[0], birthCountry=L[4], birthState=L[5], height=int(L[17])))
#print(playerRows.collect())
playerSchema = StructType([StructField('playerID', StringType(), True),\
                                      StructField('birthCountry', StringType(), True),\
                                      StructField('birthState', StringType(), True),\
                                      StructField('height', LongType(), False)])
playerDF = spark.createDataFrame(playerRows, playerSchema)
playerDF.show()
playerDF.printSchema()

playerDF.createOrReplaceTempView('player')

# COMMAND ----------

import pyspark.sql.functions as f
spark.sql("SELECT count(playerID) FROM player WHERE birthState == 'CO'").show()
playerDF.filter(f.col('birthState')=='CO').select(f.count('playerID')).show()

# COMMAND ----------

import pyspark.sql.functions as f
spark.sql("SELECT birthCountry, AVG(height) as avgHeight FROM player GROUP BY birthCountry ORDER BY avgHeight DESC").show(playerDF.count())
playerDF.groupby('birthCountry').agg(f.avg('height')).sort('avg(height)',ascending=False).show(playerDF.count())
