# Databricks notebook source
#James Godkin

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, DoubleType
import pyspark.sql.functions as f
from graphframes import *

# COMMAND ----------

routesSchema = StructType([StructField('airline', StringType(), True),\
                         StructField('airlineID', LongType(), True),\
                         StructField('sourceAirport', StringType(), True),\
                         StructField('sourceAirportID', LongType(), True),\
                         StructField('destinationAirport', StringType(), True),\
                         StructField('destinationAirportID', LongType(), True),\
                         StructField('codeshare', StringType(), True),\
                         StructField('stops', LongType(), True),\
                         StructField('planeType', StringType(), True),\
                         ])

airportsSchema = StructType([StructField('airportID', LongType(), True),\
                         StructField('name', StringType(), True),\
                         StructField('city', StringType(), True),\
                         StructField('country', StringType(), True),\
                         StructField('IATA', StringType(), True),\
                         StructField('ICAO', StringType(), True),\
                         StructField('Lat', DoubleType(), True),\
                         StructField('Long', DoubleType(), True),\
                         StructField('Alt', LongType(), True),\
                         StructField('timeZone', StringType(), True),\
                         StructField('DST', StringType(), True),\
                         StructField('databaseTimeZone', StringType(), True),\
                         StructField('type', StringType(), True),\
                         StructField('source', StringType(), True),\
                         ])

routes_df = spark.read.format('csv').option('header', True).schema(routesSchema).load("dbfs:///FileStore/tables/lab9data/routes.csv")
airports_df = spark.read.format('csv').option('header', True).schema(airportsSchema).load("dbfs:///FileStore/tables/lab9data/airports.csv")
print((routes_df.count(), len(routes_df.columns)))
routes_df.show()
print((airports_df.count(), len(airports_df.columns)))
airports_df.show()

# COMMAND ----------

nodes_df = airports_df.filter(f.col('country')!='United States')
edge_df = routes_df.join(nodes_df, routes_df['sourceAirport'] == nodes_df['IATA'], 'left_anti')
edge_df = edge_df.join(nodes_df, edge_df['destinationAirport'] == nodes_df['IATA'], 'left_anti')
nodes_df = airports_df.filter(f.col('country')=='United States').filter(f.col('IATA')!='\\N')
nodes_df = nodes_df.withColumnRenamed('IATA', 'id')
nodes_df = nodes_df[['id']]
#print((edge_df.count(), len(edge_df.columns)))
edge_df = edge_df[['sourceAirport', 'destinationAirport']].drop_duplicates()
edge_df = edge_df.withColumnRenamed('sourceAirport', 'src')
edge_df = edge_df.withColumnRenamed('destinationAirport', 'dst')
print((nodes_df.count(), len(nodes_df.columns)))
nodes_df.show()
print((edge_df.count(), len(edge_df.columns)))
edge_df.show()
edge_df.filter(f.col('src')=='DEN').show()
edge_df.filter(f.col('dst')=='DEN').show()

# COMMAND ----------

gf = GraphFrame(nodes_df, edge_df)
# gf.vertices.show()
# gf.edges.show()
gf.find("(a)-[]->(b); !(b)-[]->(a)").filter("a.id=='DEN'").show()
gf.find("(a)-[]->(b); !(b)-[]->(a)").filter("b.id=='DEN'").show()

# COMMAND ----------

results = gf.shortestPaths(landmarks=["DEN"])
results.select("id", f.explode("distances")).filter(f.col('value')>=4).orderBy('value','id').show()
