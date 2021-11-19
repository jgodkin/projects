# Databricks notebook source
#James Godkin

# COMMAND ----------

master_df = spark.read.format('csv').option('header', True).load("dbfs:///FileStore/tables/baseballdatabank/Master.csv").select('playerID','nameFirst','nameLast')
team_df = spark.read.format('csv').option('header', True).load("dbfs:///FileStore/tables/baseballdatabank/Teams.csv").select('teamID','name')
allstarFull_df = spark.read.format('csv').option('header', True).load("dbfs:///FileStore/tables/baseballdatabank/AllstarFull.csv").select('playerID','teamID')
# master_df.show()
# team_df.show()
# allstarFull_df.show()

allStar_df = master_df.join(allstarFull_df, ['playerID'], 'inner').join(team_df, ['teamID'], 'inner').distinct().sort('name')
#allStar_df.show()
allStar_df.write.format('parquet').mode('overwrite').partitionBy('name').save('allStar')

# COMMAND ----------

import pyspark.sql.functions as f
allStar = spark.read.format('parquet').load('dbfs:///allStar')
allStar.filter(f.col('name')=='Colorado Rockies').select(f.count('playerID')).show()
allStar.filter(f.col('name')=='Colorado Rockies').select('nameFirst', 'nameLast').show(allStar.count())
