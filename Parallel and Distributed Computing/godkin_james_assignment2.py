# Databricks notebook source
#James Godkin 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, DateType, BooleanType, DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import StringIndexer, StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as f

# COMMAND ----------

burritoSchema = StructType([StructField('Location', StringType(), True),\
                         StructField('Burrito', StringType(), True),\
                         StructField('Date', TimestampType(), True),\
                         StructField('Neighborhood', StringType(), True),\
                         StructField('Address', StringType(), True),\
                         StructField('URL', StringType(), True),\
                         StructField('Yelp', StringType(), True),\
                         StructField('Google', StringType(), True),\
                         StructField('Chips', StringType(), True),\
                         StructField('Cost', DoubleType(), True),\
                         StructField('Hunger', DoubleType(), True),\
                         StructField('Mass (g)', DoubleType(), True),\
                         StructField('Density (g/mL)', DoubleType(), True),\
                         StructField('Length', DoubleType(), True),\
                         StructField('Circum', DoubleType(), True),\
                         StructField('Volume', DoubleType(), True),\
                         StructField('Tortilla', DoubleType(), True),\
                         StructField('Temp', DoubleType(), True),\
                         StructField('Meat', DoubleType(), True),\
                         StructField('Fillings', DoubleType(), True),\
                         StructField('Meat:filling', DoubleType(), True),\
                         StructField('Uniformity', DoubleType(), True),\
                         StructField('Salsa', DoubleType(), True),\
                         StructField('Synergy Wrap', DoubleType(), True),\
                         StructField('overall', DoubleType(), True),\
                         StructField('Rec', BooleanType(), True),\
                         StructField('Reviewer', StringType(), True),\
                         StructField('Notes', StringType(), True),\
                         StructField('Unreliable', StringType(), True),\
                         StructField('NonSD', StringType(), True),\
                         StructField('Beef', StringType(), True),\
                         StructField('Pico', StringType(), True),\
                         StructField('Guac', StringType(), True),\
                         StructField('Cheese', StringType(), True),\
                         StructField('Sour cream', StringType(), True),\
                         StructField('Pork', StringType(), True),\
                         StructField('Chicken', StringType(), True),\
                         StructField('Shrimp', StringType(), True),\
                         StructField('Fish', StringType(), True),\
                         StructField('Rice', StringType(), True),\
                         StructField('Beans', StringType(), True),\
                         StructField('Lettuce', StringType(), True),\
                         StructField('Tomato', StringType(), True),\
                         StructField('Bell peper', StringType(), True),\
                         StructField('Carrots', StringType(), True),\
                         StructField('Cabbage', StringType(), True),\
                         StructField('Sauce', StringType(), True),\
                         StructField('Salsas', StringType(), True),\
                         StructField('Cilantro', StringType(), True),\
                         StructField('Onion', StringType(), True),\
                         StructField('Taquito', StringType(), True),\
                         StructField('Pineapple', StringType(), True),\
                         StructField('Ham', StringType(), True),\
                         StructField('Chile relleno', StringType(), True),\
                         StructField('Nopales', StringType(), True),\
                         StructField('Lobster', StringType(), True),\
                         StructField('Queso', StringType(), True),\
                         StructField('Egg', StringType(), True),\
                         StructField('Mushroom', StringType(), True),\
                         StructField('Bacon', StringType(), True),\
                         StructField('Sushi', StringType(), True),\
                         StructField('Avocado', StringType(), True),\
                         StructField('Corn', StringType(), True),\
                         StructField('Zucchini', StringType(), True),\
                         ])

burrito_df = spark.read.format('csv').option('header', True).option("mode", "DROPMALFORMED").schema(burritoSchema).load("dbfs:///FileStore/tables/Burrito").select('Date', 'Cost', 'Hunger', 'Tortilla', 'Temp', 'Meat', 'Fillings', 'Uniformity', 'Salsa', 'Synergy Wrap', 'overall', 'Pico', 'Guac', 'Cheese', 'Rice', 'Beans', 'Cilantro', 'Queso')

print((burrito_df.count(), len(burrito_df.columns)))
burrito_df.show(50)

# COMMAND ----------

burrito_df = burrito_df.filter(f.col('Date').isNotNull() & f.col('Cost').isNotNull() & f.col('Hunger').isNotNull() & f.col('Tortilla').isNotNull() & f.col('Temp').isNotNull() & f.col('Meat').isNotNull() & f.col('Fillings').isNotNull() & f.col('Uniformity').isNotNull() & f.col('Salsa').isNotNull() & f.col('Synergy Wrap').isNotNull() & f.col('overall').isNotNull())
cols = ['Pico', 'Guac', 'Cheese', 'Rice', 'Beans', 'Cilantro', 'Queso']
for col in cols:
    burrito_df = burrito_df.withColumn(col, 
        f.when(f.col(col) == 'x',
            'yes'
        ).when(f.col(col) == 'X',
            'yes'
        ).otherwise(f.col(col).cast('string'))
    )
burrito_df = burrito_df.na.fill(value='no')
print((burrito_df.count(), len(burrito_df.columns)))
burrito_df.show(50)

# COMMAND ----------

burrito_test_df, burrito_train_df = burrito_df.randomSplit([0.3, 0.7])
print((burrito_test_df.count(), len(burrito_test_df.columns)))
print((burrito_train_df.count(), len(burrito_train_df.columns)))

# COMMAND ----------

rfr = RandomForestRegressor(labelCol='overall', seed=52637)

pico_indexer = StringIndexer(inputCol='Pico', outputCol='PicoIndex')
guac_indexer = StringIndexer(inputCol='Guac', outputCol='GuacIndex')
cheese_indexer = StringIndexer(inputCol='Cheese', outputCol='CheeseIndex')
rice_indexer = StringIndexer(inputCol='Rice', outputCol='RiceIndex')
beans_indexer = StringIndexer(inputCol='Beans', outputCol='BeansIndex')
cilantro_indexer = StringIndexer(inputCol='Cilantro', outputCol='CilantroIndex')
queso_indexer = StringIndexer(inputCol='Queso', outputCol='QuesoIndex')
# picoTrans = pico_indexer.fit(burrito_train_df).transform(burrito_train_df)
# picoTrans.show()
feature_vec = VectorAssembler(inputCols=['Cost', 'Hunger', 'Tortilla', 'Temp', 'Meat', 'Fillings', 'Uniformity', 'Salsa', 'Synergy Wrap', 'PicoIndex', 'GuacIndex', 'CheeseIndex', 'RiceIndex', 'BeansIndex', 'CilantroIndex', 'QuesoIndex'], outputCol='features')

Stages = [pico_indexer, guac_indexer, cheese_indexer, rice_indexer, beans_indexer, cilantro_indexer, queso_indexer, feature_vec, rfr]
rfr_pipeline = Pipeline(stages=Stages)
rfr_model = rfr_pipeline.fit(burrito_train_df)
predict = rfr_model.transform(burrito_train_df)
predict.select('overall', 'prediction').show()
evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='overall')
print('Train RMSE:',evaluator.evaluate(predict))
predict = rfr_model.transform(burrito_test_df)
predict.select('overall', 'prediction').show()
evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='overall')
print('Test RMSE:',evaluator.evaluate(predict))
# pipelinePath = "/tmp/rfr-pipeline-model"
# rfr_model.write().overwrite().save(pipelinePath)

# COMMAND ----------

dbutils.fs.rm("FileStore/tables/burrito_test", True)

print(burrito_test_df.rdd.getNumPartitions())
burrito_test_df = burrito_test_df.orderBy('Date').repartition(15).persist()
print(burrito_test_df.rdd.getNumPartitions())
dbutils.fs.rm('FileStore/tables/burrito_test/', True)
burrito_test_df.write.format('csv').option('header', True).save('FileStore/tables/burrito_test/')

# COMMAND ----------

#Source
burrito_stream = spark.readStream.format('csv').option('header', True).option("mode", "DROPMALFORMED").option('maxFilesPerTrigger', 1).schema(burritoSchema).load("dbfs:///FileStore/tables/burrito_test").select('Cost', 'Hunger', 'Tortilla', 'Temp', 'Meat', 'Fillings', 'Uniformity', 'Salsa', 'Synergy Wrap', 'overall', 'Pico', 'Guac', 'Cheese', 'Rice', 'Beans', 'Cilantro', 'Queso')
#Query
predict = rfr_model.transform(burrito_stream)
#Sink
burrito_sink = predict.writeStream.outputMode('append').format('memory').queryName('burrito_test_predict').start()
#burrito_sink.awaitTermination()

# COMMAND ----------

spark.sql('select "features", "prediction" from burrito_test_predict').show(10)

# COMMAND ----------

#The above is everything we learned in class the stream doesn't work not sure why. Below is model pipeline streaming from the book it mostly works, but is making the overall score null not sure why. The streaming is working, but only on values that are doubles.

# COMMAND ----------

smallBurritoSchema = StructType([StructField('Date', DoubleType(), True),\
                         StructField('Cost', DoubleType(), True),\
                         StructField('Hunger', DoubleType(), True),\
                         StructField('Tortilla', DoubleType(), True),\
                         StructField('Temp', DoubleType(), True),\
                         StructField('Meat', DoubleType(), True),\
                         StructField('Fillings', DoubleType(), True),\
                         StructField('Uniformity', DoubleType(), True),\
                         StructField('Salsa', DoubleType(), True),\
                         StructField('Synergy Wrap', DoubleType(), True),\
                         StructField('overall', DoubleType(), True),\
                         StructField('Pico', StringType(), True),\
                         StructField('Guac', StringType(), True),\
                         StructField('Cheese', StringType(), True),\
                         StructField('Rice', StringType(), True),\
                         StructField('Beans', StringType(), True),\
                         StructField('Cilantro', StringType(), True),\
                         StructField('Queso', StringType(), True),\
                         ])

# COMMAND ----------

rfr = RandomForestRegressor(labelCol='overall', seed=52637)
# Scaling the data makes the RMSE worse
# feature_vec = VectorAssembler(inputCols=['Cost', 'Hunger', 'Tortilla', 'Temp', 'Meat', 'Fillings', 'Uniformity', 'Salsa', 'Synergy Wrap'], outputCol='feature')
# scaledFeatures = StandardScaler(inputCol="feature", outputCol="features")
feature_vec = VectorAssembler(inputCols=['Cost', 'Hunger', 'Tortilla', 'Temp', 'Meat', 'Fillings', 'Uniformity', 'Salsa', 'Synergy Wrap'], outputCol='features')

#tages = [feature_vec, scaledFeatures, rfr]
Stages = [feature_vec, rfr]
rfr_pipeline = Pipeline(stages=Stages)
rfr_model = rfr_pipeline.fit(burrito_train_df)
predict = rfr_model.transform(burrito_train_df)
predict.select('overall', 'prediction').show()
evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='overall')
print('Train RMSE:',evaluator.evaluate(predict))
predict = rfr_model.transform(burrito_test_df)
predict.select('overall', 'prediction').show()
evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='overall')
print('Test RMSE:',evaluator.evaluate(predict))

# COMMAND ----------

import mlflow.spark
#pipelineModel = mlflow.spark.load_model(pipelinePath)
repartitionedPath = "dbfs:///FileStore/tables/burrito_test"
schema = smallBurritoSchema
streamingData = (spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).option('header', True).csv(repartitionedPath).select('Cost', 'Hunger', 'Tortilla', 'Temp', 'Meat', 'Fillings', 'Uniformity', 'Salsa', 'Synergy Wrap', 'overall'))
# Generate predictions
streamPred = rfr_model.transform(streamingData)
burrito_sink = streamPred.writeStream.outputMode('append').format('memory').queryName('new_burrito_test_predict').start()

# COMMAND ----------

spark.sql('select* from new_burrito_test_predict').show(10)
# evaluate['prediction'] = spark.sql('select prediction from new_burrito_test_predict')
evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='overall')
print('Test RMSE:',evaluator.evaluate(predict))
