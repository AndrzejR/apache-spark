from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("script_one")
sc = SparkContext(conf = conf)

rdd = sc.textFile("c:/SparkCourse/fakefriends.csv")
numbers_of_friends = rdd.map(lambda x: int(x.split(',')[3]))
sum_of_friends = numbers_of_friends.reduce(lambda x,y: x+y)
count_of_friends = numbers_of_friends.count()

print("Average number of friends is: " + str(sum_of_friends/count_of_friends))
