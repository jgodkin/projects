# Databricks notebook source
#James Godkin

# COMMAND ----------

import math
threshold = 0.75

# COMMAND ----------

txtFile = "dbfs:///FileStore/tables/assignment1Data"
file = sc.textFile(txtFile)
points = file.map(lambda x: (x.split(',')))
print(points.collect())

# COMMAND ----------

#Clean Version
#Brute-Force
point = points.collect()
near_points=[]
for i in range(len(point)-1):
  for j in range(i+1, len(point)):
    x2 = float(point[j][1])
    x1 = float(point[i][1])
    y2 = float(point[j][2])
    y1 = float(point[i][2])
    dist=((((x2-x1)**2)+((y2-y1)**2))**0.5)
    if dist <= threshold:
      near_points.append((point[i][0], point[j][0]))  
#Build The Grid
grid = points.map(lambda point: (point[0], (float(point[1]), float(point[2])), ((math.floor(float(point[1])/threshold)),math.floor(float(point[2])/threshold))))
grid = grid.map(lambda ref: [ref[2],([(ref[0], ref[1])])])
grid = grid.reduceByKey(lambda x, y: x+y).persist()
#Send The Points To The Right Grid
back1up1 = grid.map(lambda x: ((x[0][0]-1, x[0][1]+1), x[1]))
up1 = grid.map(lambda x: ((x[0][0], x[0][1]+1), x[1]))
forw1up1 = grid.map(lambda x: ((x[0][0]+1, x[0][1]+1), x[1]))
forw1 = grid.map(lambda x: ((x[0][0]+1, x[0][1]+0), x[1]))
#This Should Be A Loop Not Copy and Pasted 4 Times
grid = grid.leftOuterJoin(back1up1)
grid = grid.flatMapValues(lambda x: x)
grid = grid.filter(lambda x: x[1]!=None)
grid = grid.reduceByKey(lambda x, y: x+y)

grid = grid.leftOuterJoin(up1)
grid = grid.flatMapValues(lambda x: x)
grid = grid.filter(lambda x: x[1]!=None)
grid = grid.reduceByKey(lambda x, y: x+y)

grid = grid.leftOuterJoin(forw1up1)
grid = grid.flatMapValues(lambda x: x)
grid = grid.filter(lambda x: x[1]!=None)
grid = grid.reduceByKey(lambda x, y: x+y)

grid = grid.leftOuterJoin(forw1)
grid = grid.flatMapValues(lambda x: x)
grid = grid.filter(lambda x: x[1]!=None)
grid = grid.reduceByKey(lambda x, y: x+y)

gridded = grid.sortByKey()
gridded = gridded.mapValues(lambda x: (sorted(x))).persist()
#Calculate Distance Between Points
grid2points = gridded.filter(lambda points: len(points[1])>1).sortByKey() #Filter Out All Single Points
closest_points = grid2points.map(lambda points: (points[0], [((points[1][i][0], points[1][j][0]), ((((points[1][j][1][0]-points[1][i][1][0])**2)+((points[1][j][1][1]-points[1][i][1][1])**2))**0.5)) for i in range(len(point[1])) for j in range(i+1, len(points[1]))]))
#Combine And Remove Duplicates
closest_points = closest_points.flatMap(lambda points: points[1])
closest_points = closest_points.filter(lambda points: points[1]<=threshold)
closest_points = closest_points.reduceByKey(lambda a,b: a+b)
closest_points = closest_points.map(lambda points: points[0])
#Print Results
print('Threshold:', threshold)
print('Brute-Force:', len(near_points), near_points)   
print('Grid Processing:', len(near_points),closest_points.sortByKey().collect())

# COMMAND ----------

#Beware Below This Point Is All The Trail And Error

# COMMAND ----------

# #Brute-force
# point = points.collect()
# near_points=[]
# for i in range(len(point)-1):
#   for j in range(i+1, len(point)):
#     x2 = float(point[j][1])
#     x1 = float(point[i][1])
#     y2 = float(point[j][2])
#     y1 = float(point[i][2])
#     dist=((((x2-x1)**2)+((y2-y1)**2))**0.5)
#     if dist <= threshold:
#       near_points.append((point[i][0], point[j][0]))
# print('Threshold:', threshold)
# print(len(near_points), near_points)

# COMMAND ----------

# grid = points.map(lambda point: (point[0], (float(point[1]), float(point[2])), ((math.floor(float(point[1])/threshold)),math.floor(float(point[2])/threshold))))
# grid = grid.map(lambda ref: [ref[2],([(ref[0], ref[1])])])
# # print(grid.keys().collect())
# # print(grid.collect())
# # print(" ")
# grid = grid.reduceByKey(lambda x, y: x+y).persist()
# # grid = grid.sortByKey()
# # print(grid.collect())
# # print(" ")
# #grid = grid.groupByKey().mapValues(list)
# # # grid = grid.sortByKey()
# #grid = grid.mapValues(lambda x: (len(x), x))
# # print(grid.collect())
# # # print(gridded.keys().collect())
# # # print(gridded.glom().collect())
# back1up1 = grid.map(lambda x: ((x[0][0]-1, x[0][1]+1), x[1]))
# # print(back1up1.collect())
# up1 = grid.map(lambda x: ((x[0][0], x[0][1]+1), x[1]))
# # print(up1.collect())
# forw1up1 = grid.map(lambda x: ((x[0][0]+1, x[0][1]+1), x[1]))
# # print(forw1up1.collect())
# forw1 = grid.map(lambda x: ((x[0][0]+1, x[0][1]+0), x[1]))
# # print(forw1.collect())
# grid = grid.leftOuterJoin(back1up1)
# # print(grid.collect())
# # print(" ")
# grid = grid.flatMapValues(lambda x: x)
# grid = grid.filter(lambda x: x[1]!=None)
# # # print(grid.collect())
# # grid = grid.groupByKey().mapValues(list)
# grid = grid.reduceByKey(lambda x, y: x+y)
# grid = grid.leftOuterJoin(up1)
# # grid = grid.groupByKey().mapValues(list)
# # print(grid.collect())
# # print(" ")
# grid = grid.flatMapValues(lambda x: x)
# # print(grid.collect())
# grid = grid.filter(lambda x: x[1]!=None)
# # #print(grid.collect())
# # #grid = grid.mapValues(lambda x: [x])
# # # print(grid.collect())
# grid = grid.reduceByKey(lambda x, y: x+y)
# # # # grid = grid.flatMapValues(lambda x: [x])
# # # # # print(grid.collect())
# grid = grid.leftOuterJoin(forw1up1)
# grid = grid.flatMapValues(lambda x: x)
# grid = grid.filter(lambda x: x[1]!=None)
# grid = grid.reduceByKey(lambda x, y: x+y)
# # # # grid = grid.flatMapValues(lambda x: x)
# # # # grid = grid.groupByKey().mapValues(list)
# # # # # print(grid.collect())
# # # # #grid = grid.groupByKey().mapValues(list)
# grid = grid.leftOuterJoin(forw1)
# grid = grid.flatMapValues(lambda x: x)
# grid = grid.filter(lambda x: x[1]!=None)
# grid = grid.reduceByKey(lambda x, y: x+y)
# # # grid = grid.flatMapValues(lambda x: x)
# # # grid = grid.groupByKey().mapValues(list)
# # # print(grid.collect())
# # #gridded = grid.groupByKey().mapValues(list)
# # print(" ")
# # grid = grid.leftOuterJoin(back1up1)
# # grid = grid.flatMapValues(lambda x: x)
# # grid = grid.filter(lambda x: x[1]!=None)
# # grid = grid.reduceByKey(lambda x, y: x+y)

# # testgrid = grid.leftOuterJoin(up1)
# # testgrid = testgrid.flatMapValues(lambda x: x)
# # testgrid = testgrid.filter(lambda x: x[1]!=None)

# # testgrid = testgrid.reduceByKey(lambda x, y: x+y)
# #grid = grid.flatMapValues(lambda x: x)
# gridded = grid.sortByKey()
# # print(gridded.collect())
# gridded = gridded.mapValues(lambda x: (sorted(x))).persist()
# # griddedtest = testgrid.sortByKey()
# print(gridded.collect())
# # print(" ")
# # print(griddedtest.collect())
# # print(" ")

# COMMAND ----------

# [((0, 0), [('Pt11', (0.68422, 0.734435))]),
#  ((0, 2), [('Pt06', (0.0127773, 2.04053)), ('Pt09', (0.00945363, 1.83433))]),
#  ((1, 0), [('Pt17', (1.05066, 0.355635))]),
#  ((1, 6), [('Pt02', (1.49829, 5.05809))]),
#  ((3, 1), [('Pt19', (2.33941, 0.867107))]),
#  ((3, 4), [('Pt14', (2.75711, 3.04256))]),
#  ((4, 0), [('Pt00', (3.49838, 0.662006))]),
#  ((4, 3), [('Pt13', (3.37496, 2.49623))]),
#  ((4, 4), [('Pt01', (3.0474, 3.20142)), ('Pt15', (3.7311, 3.22135))]),
#  ((4, 5), [('Pt07', (3.53014, 3.80594))]),
#  ((4, 6), [('Pt03', (3.55013, 4.96649)), ('Pt10', (3.18407, 4.52642))]),
#  ((5, 2), [('Pt16', (3.93387, 1.98305))]), 
#  ((5, 4), [('Pt05', (3.88558, 3.55233))]),
#  ((5, 7), [('Pt08', (4.15061, 5.80679))]),
#  ((6, 2), [('Pt18', (4.99483, 1.53335))]),
#  ((6, 5), [('Pt04', (4.84535, 4.05086))]),
#  ((7, 3), [('Pt12', (5.31465, 2.48444))])]

# [((0, 0), [('Pt11', (0.68422, 0.734435))]),
#  ((0, 2), [('Pt06', (0.0127773, 2.04053)), ('Pt09', (0.00945363, 1.83433))]),
#  ((1, 0), [('Pt17', (1.05066, 0.355635)), ('Pt11', (0.68422, 0.734435))]),
#  ((1, 6), [('Pt02', (1.49829, 5.05809))]),
#  ((3, 1), [('Pt19', (2.33941, 0.867107)), ('Pt00', (3.49838, 0.662006))]),
#  ((3, 4), [('Pt14', (2.75711, 3.04256)), ('Pt13', (3.37496, 2.49623))]),
#  ((4, 0), [('Pt00', (3.49838, 0.662006))]),
#  ((4, 3), [('Pt13', (3.37496, 2.49623)), ('Pt16', (3.93387, 1.98305))]),
#  ((4, 4), [('Pt01', (3.0474, 3.20142)), ('Pt15', (3.7311, 3.22135)), ('Pt13', (3.37496, 2.49623)), ('Pt14', (2.75711, 3.04256))]),
#  ((4, 5), [('Pt07', (3.53014, 3.80594)), ('Pt05', (3.88558, 3.55233)), ('Pt01', (3.0474, 3.20142)), ('Pt15', (3.7311, 3.22135)), ('Pt14', (2.75711, 3.04256))]),
#  ((4, 6), [('Pt03', (3.55013, 4.96649)), ('Pt10', (3.18407, 4.52642)), ('Pt07', (3.53014, 3.80594))]),
#  ((5, 2), [('Pt16', (3.93387, 1.98305))]),
#  ((5, 4), [('Pt05', (3.88558, 3.55233)), ('Pt13', (3.37496, 2.49623)), ('Pt01', (3.0474, 3.20142)), ('Pt15', (3.7311, 3.22135))]),
#  ((5, 7), [('Pt08', (4.15061, 5.80679)), ('Pt03', (3.55013, 4.96649)), ('Pt10', (3.18407, 4.52642))]),
#  ((6, 2), [('Pt18', (4.99483, 1.53335)), ('Pt16', (3.93387, 1.98305))]),
#  ((6, 5), [('Pt04', (4.84535, 4.05086)), ('Pt05', (3.88558, 3.55233))]),
#  ((7, 3), [('Pt12', (5.31465, 2.48444)), ('Pt18', (4.99483, 1.53335))])]

# [((0, 0), [('Pt11', (0.68422, 0.734435))]),
#  ((0, 2), [('Pt06', (0.0127773, 2.04053)), ('Pt09', (0.00945363, 1.83433))]),
#  ((1, 0), [('Pt11', (0.68422, 0.734435)), ('Pt17', (1.05066, 0.355635))]),
#  ((1, 6), [('Pt02', (1.49829, 5.05809))]),
#  ((3, 1), [('Pt00', (3.49838, 0.662006)), ('Pt19', (2.33941, 0.867107))]),
#  ((3, 4), [('Pt13', (3.37496, 2.49623)), ('Pt14', (2.75711, 3.04256))]),
#  ((4, 0), [('Pt00', (3.49838, 0.662006))]),
#  ((4, 3), [('Pt13', (3.37496, 2.49623)), ('Pt16', (3.93387, 1.98305))]), 
#  ((4, 4), [('Pt01', (3.0474, 3.20142)), ('Pt13', (3.37496, 2.49623)), ('Pt14', (2.75711, 3.04256)), ('Pt15', (3.7311, 3.22135))]),
#  ((4, 5), [('Pt01', (3.0474, 3.20142)), ('Pt05', (3.88558, 3.55233)), ('Pt07', (3.53014, 3.80594)), ('Pt14', (2.75711, 3.04256)), ('Pt15', (3.7311, 3.22135))]),
#  ((4, 6), [('Pt03', (3.55013, 4.96649)), ('Pt07', (3.53014, 3.80594)), ('Pt10', (3.18407, 4.52642))]),
#  ((5, 2), [('Pt16', (3.93387, 1.98305))]),
#  ((5, 4), [('Pt01', (3.0474, 3.20142)), ('Pt05', (3.88558, 3.55233)), ('Pt13', (3.37496, 2.49623)), ('Pt15', (3.7311, 3.22135))]),
#  ((5, 7), [('Pt03', (3.55013, 4.96649)), ('Pt08', (4.15061, 5.80679)), ('Pt10', (3.18407, 4.52642))]),
#  ((6, 2), [('Pt16', (3.93387, 1.98305)), ('Pt18', (4.99483, 1.53335))]),
#  ((6, 5), [('Pt04', (4.84535, 4.05086)), ('Pt05', (3.88558, 3.55233))]),
#  ((7, 3), [('Pt12', (5.31465, 2.48444)), ('Pt18', (4.99483, 1.53335))])]

# COMMAND ----------

# # def bruteForce(points):
# #   for i in range(0,len(points[1])-1):
# #     for j in range(i+1, len(points[1])):
# #       x2 = points[1][j][1][0]
# #       x1 = points[1][i][1][0]
# #       y2 = points[1][j][1][1]
# #       y1 = points[1][i][1][1]
# #       dist=((((x2-x1)**2)+((y2-y1)**2))**0.5)
# #   return [points[0], (points[1][i][0], points[1][j][0]), dist]
  
# # test2 = gridded.map(lambda points: len(points[1]))
# # print(test2.collect())
# grid2points = gridded.filter(lambda points: len(points[1])>1).sortByKey()
# # test2 = grid2points.map(lambda points: len(points[1]))
# # print(test2.collect())
# # print(test.collect())
# # for i in test.keys().collect():
# #   gribPoint = test.filter(lambda points: points[0]==i)
# #   gribPoint = gribPoint.map(lambda x: x[1][0][0])
# #   print(gribPoint.collect())
# # test = test.flatMapValues(lambda points: points)
# # test = test.groupByKey().mapValues(list)
# # print(test.collect())
# # print(" ")
# # test2 = test.map(lambda points: (points[1]))#for i in range(len(points[1])) #points[1][i][0] point name points[1][i][1][0]=x points[1][i][1][1]=y
# # print(test2.collect())
# # print(" ")
# # test2 = test2.map(lambda points: (points[0],points[1][0],points[1][1]))
# # print(test2.collect())
# # print(" ")
# # test3 = test2.map(lambda points: points[2][1])
# # print(test3.collect())
# # print(" ")
# closest_points = grid2points.map(lambda points: (points[0], [((points[1][i][0], points[1][j][0]), ((((points[1][j][1][0]-points[1][i][1][0])**2)+((points[1][j][1][1]-points[1][i][1][1])**2))**0.5)) for i in range(len(point[1])) for j in range(i+1, len(points[1]))]))
# # print(closest_points.sortByKey().collect())
# # print(' ')
# closest_points = closest_points.flatMap(lambda points: points[1])
# # print(closest_points.sortByKey().collect())
# # print(' ')
# closest_points = closest_points.filter(lambda points: points[1]<=threshold)
# closest_points = closest_points.reduceByKey(lambda a,b: a+b)
# closest_points = closest_points.map(lambda points: points[0])
# print('Threshold:', threshold)
# print('Brute-Force:', len(near_points), near_points)   
# print('Grid Processing:', len(near_points),closest_points.sortByKey().collect())
# #('Pt01', 'Pt14'), ('Pt01', 'Pt15'), ('Pt03', 'Pt10'), ('Pt05', 'Pt07'), ('Pt05', 'Pt15'), ('Pt06', 'Pt09'), ('Pt07', 'Pt15'), ('Pt11', 'Pt17')]
