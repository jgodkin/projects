# Databricks notebook source
#James Godkin

# COMMAND ----------

#dbutils.fs.rm("FileStore/tables/lab3short", True)
dbutils.fs.mkdirs("FileStore/tables/lab3short")

fileCount=sc.binaryFiles("FileStore/tables/Lab3Short").count()
for i in range(fileCount):
  dbutils.fs.cp("FileStore/tables/Lab3Short/shortLab3data{}.txt".format(str(i)), "FileStore/tables/lab3short")


# COMMAND ----------

#dbutils.fs.rm("FileStore/tables/lab3full", True)
dbutils.fs.mkdirs("FileStore/tables/lab3full")

fileCount=sc.binaryFiles("FileStore/tables/Lab3Full").count()
for i in range(fileCount):
  dbutils.fs.cp("FileStore/tables/Lab3Full/fullLab3data{}.txt".format(str(i)), "FileStore/tables/lab3full")

# COMMAND ----------

txtFile = "dbfs:///FileStore/tables/lab3short"
#txtFile = "dbfs:///FileStore/tables/lab3full"

file = sc.textFile(txtFile)
#print(file.collect())
links = file.map(lambda link: (link.split()))
#print(links.collect())
wedpage_fromRef = links.flatMap(lambda ref: [(ref[i],ref[0]) for i in range(1,len(ref),1)])
#print(wedpage_fromRef.collect())
webpage_ref = wedpage_fromRef.groupByKey().mapValues(list) #got the .mapValues(list) off of stackoverflow: https://stackoverflow.com/questions/29717257/pyspark-groupbykey-returning-pyspark-resultiterable-resultiterable
#print(webpage_ref.collect())
page_ref_sortVal = webpage_ref.mapValues(lambda x: sorted(set(x))) #set gets unique values
#print(page_ref_sortVal.collect())
page_ref_sort = page_ref_sortVal.sortByKey()
print(page_ref_sort.collect())

# COMMAND ----------

txtFile = "dbfs:///FileStore/tables/lab3full"

file = sc.textFile(txtFile)
links = file.map(lambda link: (link.split()))
wedpage_fromRef = links.flatMap(lambda ref: [(ref[i],ref[0]) for i in range(1,len(ref),1)])
webpage_ref = wedpage_fromRef.groupByKey().mapValues(list) #got the .mapValues(list) off of stackoverflow: https://stackoverflow.com/questions/29717257/pyspark-groupbykey-returning-pyspark-resultiterable-resultiterable
page_ref_sortVal = webpage_ref.mapValues(lambda x: sorted(set(x)))
page_ref_sort = page_ref_sortVal.sortByKey()
print("Count:",page_ref_sort.count())
print(page_ref_sort.take(10))
