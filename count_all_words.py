"""How many words are there in a document? A morning warm-up for Spark."""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('CountAllWords')
sc = SparkContext(conf=conf)

lines = sc.textFile('c:/SparkCourse/to_count.txt')
parsed_lines = lines.flatMap(lambda x: x.split())

result = parsed_lines.count()

print(result)
