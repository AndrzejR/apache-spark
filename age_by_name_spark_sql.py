""" Age by name spark-sql version.
Get the average age of people with a given name and their count.
Data:
0,Will,33,385
1,Jean-Luc,26,2
2,Hugh,55,221
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row

conf = SparkConf().setMaster("local").setAppName("AgeByName_SparkSQL")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def parse(line):
	fields = line.split(',')
	return Row(ID=int(fields[0]), name=str(fields[1]), age=int(fields[2]))

lines = sc.textFile("c:/SparkCourse/fakefriends.csv")
parsed_lines = lines.map(parse)

sql_rdd = sqlContext.createDataFrame(parsed_lines)
sql_rdd.registerTempTable("friends")

age_agg = sqlContext.sql("SELECT name, avg(age) as avg_age, count(ID) as name_count FROM friends GROUP BY name ORDER BY name_count")


with open("c:/SparkCourse/name_age_averages_sql.txt", "w") as f:	
	for age_agg_row in age_agg.collect():
		f.write(str(age_agg_row)+"\n")
