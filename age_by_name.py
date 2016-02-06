'''My second spark script: get the average age of people with a given name and their count.
'''
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("age_by_name")
sc = SparkContext(conf=conf)

def parseline(line):
	arr = line.split(',')
	name = arr[1]
	age  = int(arr[2])
	return (name, age)

rdd = sc.textFile("c:/SparkCourse/fakefriends.csv")
name_age = rdd.map(parseline)
sums_by_names = name_age.mapValues(lambda x: (x, 1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

#let's get not only the averages, but also keep the counts of people per name
avgs_by_names = sums_by_names.mapValues(lambda x: (x[1], x[0]/x[1]))

sorted_by_count = sorted(avgs_by_names.collect(), key=lambda x: x[1])

print("Name, Count, Average Age")
for element in sorted_by_count:
	print("{}, {}, {:.2f}".format(element[0], element[1][0], element[1][1]))
