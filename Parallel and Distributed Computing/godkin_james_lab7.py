# Databricks notebook source
#James Godkin

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, Bucketizer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pyspark.sql.functions as f

# COMMAND ----------

heartSchema = StructType([StructField('id', LongType(), True),\
                                      StructField('age', LongType(), True),\
                                      StructField('sex', StringType(), True),\
                                      StructField('chol', LongType(), True),\
                                      StructField('pred', StringType(), True)])

train_df = spark.read.format('csv').option('header', True).schema(heartSchema).load("dbfs:///FileStore/tables/lab7data/heartTraining.csv")
test_df = spark.read.format('csv').option('header', True).schema(heartSchema).load("dbfs:///FileStore/tables/lab7data/heartTesting.csv")

train_df.show()
test_df.show()

# COMMAND ----------

lr = LogisticRegression(maxIter=10, regParam=0.01)

age_split = [-float('inf'), 40, 50, 60, 70, float('inf')]
age_bucket = Bucketizer(splits=age_split, inputCol='age', outputCol='ageBucket')
# ageTrans = age_bucket.transform(train_df)
# ageTrans.show()
sex_indexer = StringIndexer(inputCol='sex', outputCol='sexIndex')
# sexTrans = sex_indexer.fit(train_df).transform(train_df)
# sexTrans.show()
pred_indexer = StringIndexer(inputCol='pred', outputCol='label')
# predTrans = pred_indexer.fit(train_df).transform(train_df)
# predTrans.show()
feature_vec = VectorAssembler(inputCols=['ageBucket', 'sexIndex', 'chol'], outputCol='features')

Stages = [age_bucket, sex_indexer, pred_indexer, feature_vec, lr]
lr_pipline = Pipeline(stages=Stages)
lr_model = lr_pipline.fit(train_df)
predict = lr_model.transform(test_df)
predict.select('id','probability','prediction').show()

# COMMAND ----------

predict_train = lr_model.transform(train_df)
# predict_train.show()
correct_pred = predict_train.filter(f.col('label')==f.col('prediction')).select(f.count('id'))
total = predict_train.select(f.count('prediction'))
accuracy_train = correct_pred.join(total).withColumn('accuracy_train', (f.col('count(id)') / f.col('count(prediction)'))).drop('count(id)', 'count(prediction)')

correct_pred = predict.filter(f.col('label')==f.col('prediction')).select(f.count('id'))
total = predict.select(f.count('prediction'))
accuracy = correct_pred.join(total).withColumn('accuracy', (f.col('count(id)') / f.col('count(prediction)'))).drop('count(id)', 'count(prediction)')

accuracy_train.show()
accuracy.show()

evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction')
print('Area Under the Curve Train:', evaluator.evaluate(predict_train))
print('Area Under the Curve  Test:', evaluator.evaluate(predict))
