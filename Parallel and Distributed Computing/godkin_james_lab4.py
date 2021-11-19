# Databricks notebook source
txtFile = "dbfs:///FileStore/tables/lab3short"
file = sc.textFile(txtFile)

#file = sc.parallelize(["a b c", "b a a", "c b", "d a"])

links = file.map(lambda link: (link.split()))
wedpage_fromRef = links.flatMap(lambda ref: [(ref[0],ref[i]) for i in range(1,len(ref),1)])
webpage_ref = wedpage_fromRef.groupByKey().mapValues(list)
page_ref_sortVal = webpage_ref.mapValues(lambda x: sorted(set(x)))
page_ref_sort = page_ref_sortVal.sortByKey()

page_point = page_ref_sort.map(lambda key: (key[0], 1))
keycount = page_point.count()
page_points = page_point.map(lambda points: (points[0], points[1]/keycount))

page_rank=page_ref_sort.join(page_points)
for i in range(10):
  #print("Iteration:", i)
  #print(page_rank.collect())
  new_page_point = page_rank.flatMap(lambda point: [(point[1][0][i],point[1][1]/len(point[1][0])) for i in range(len(point[1][0]))])
  #print(new_page_point.collect())
  new_page_points = new_page_point.reduceByKey(lambda a,b: a+b)
  #print(new_page_points.collect())
  page_rank=page_ref_sort.join(new_page_points)
sort_page_rank = new_page_points.sortBy(lambda pair:pair[1],False)
print('Final sorted rankings:', sort_page_rank.collect())
