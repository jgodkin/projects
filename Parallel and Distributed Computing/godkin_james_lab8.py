# Databricks notebook source
#James Godkin

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
import pyspark.sql.functions as f

# COMMAND ----------

fifaSchema = StructType([StructField('ID', LongType(), True),\
                         StructField('lang', StringType(), True),\
                         StructField('Date', TimestampType(), True),\
                         StructField('Source', StringType(), True),\
                         StructField('len', LongType(), True),\
                         StructField('Orig_Tweet', StringType(), True),\
                         StructField('Tweet', StringType(), True),\
                         StructField('Likes', LongType(), True),\
                         StructField('RTs', LongType(), True),\
                         StructField('Hashtags', StringType(), True),\
                         StructField('UserMentionNames', StringType(), True),\
                         StructField('UserMentionID', StringType(), True),\
                         StructField('Name', StringType(), True),\
                         StructField('Place', StringType(), True),\
                         StructField('Followers', LongType(), True),\
                         StructField('Friends', LongType(), True),\
                         ])
fifa_df = spark.read.format('csv').option('header', True).option("mode", "DROPMALFORMED").schema(fifaSchema).load("dbfs:///FileStore/tables/FIFA.csv")
fifa_df.show()

# COMMAND ----------

print(fifa_df.rdd.getNumPartitions())
fifa_df = fifa_df.orderBy('Date').repartition(50).persist()
print(fifa_df.rdd.getNumPartitions())
#print(fifa_df.rdd.glom().collect())
dbutils.fs.rm('FileStore/tables/fifa/', True)
fifa_df.write.format('csv').option('header', True).save('FileStore/tables/fifa/')

# COMMAND ----------

fifa_df = spark.read.format('csv').option('header', True).option("mode", "DROPMALFORMED").schema(fifaSchema).load("dbfs:///FileStore/tables/fifa").select('ID', 'Date', 'Hashtags')
fifa_df = fifa_df.filter(f.col('Hashtags').isNotNull()).withColumn('Hashtags',f.explode(f.split('Hashtags',','))).groupby(f.window('Date','60 minutes', '30 minutes'), f.col('Hashtags')).agg(f.count('ID')).filter(f.col('count(ID)')>=100).orderBy(f.col('window').desc(), f.col('count(ID)').desc())
fifa_df.show()

# COMMAND ----------

#Source
fifa_stream = spark.readStream.format('csv').option('header', True).option("mode", "DROPMALFORMED").option('maxFilesPerTrigger', 1).schema(fifaSchema).load("dbfs:///FileStore/tables/fifa").select('ID', 'Date', 'Hashtags')
#Query
fifa_top_hashtags = fifa_stream.withWatermark('Date', '24 hours').filter(f.col('Hashtags').isNotNull()).withColumn('Hashtags',f.explode(f.split('Hashtags',','))).groupby(f.window('Date','60 minutes', '30 minutes'), f.col('Hashtags')).agg(f.count('ID')).filter(f.col('count(ID)')>=100).orderBy(f.col('window'), f.col('count(ID)').desc())
#Sink
fifa_sink = fifa_top_hashtags.writeStream.outputMode('complete').format('memory').queryName('fifa_top_hashtags').trigger(processingTime='10 seconds').start()

# COMMAND ----------

spark.sql('select* from fifa_top_hashtags').show(10)
