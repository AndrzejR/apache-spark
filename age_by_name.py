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
# name_grouped, (sum_ages, count)

#let's get not only the averages, but also keep the counts of people per name
avgs_by_names = sums_by_names.mapValues(lambda x: (x[1], x[0]/x[1]))
# name_grouped, (count, avg_age)

# make the sort scalable
sorted_by_count = avgs_by_names.map(lambda x_y: (x_y[1], x_y[0])).sortByKey()
# (count, avg_age), name_grouped

result = sorted_by_count.collect()

print("Name, Count, Average Age")
for element in result:
	print("{}, {}, {:.2f}".format(element[1], element[0][0], element[0][1]))
