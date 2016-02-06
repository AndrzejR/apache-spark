"""Most popular movie from the movie db.
Example data:
UserID, MovieID, Rating, Timestamp
196	242	3	881250949
186	302	3	891717742
22	377	1	878887116
"""
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('most_popular_movie')
sc = SparkContext(conf=conf)

lines = sc.textFile('c:/SparkCourse/ml-100k/u.data')
parse_lines = lines.map(lambda x: (int(x.split()[1]), int(x.split()[2])))
# MovieID, Rating

prep_ratings = parse_lines.mapValues(lambda x: (x,1))
# MovieID, (Rating, 1)

sums_ratings = prep_ratings.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
# MovieID_grouped, (Rating_sum, Count)

average_ratings = sums_ratings.mapValues(lambda x: x[0]/x[1])
# MovieID_grouped, average_rating

the_best_movie = average_ratings.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).first()

print("MovieID: " + str(the_best_movie[1]) + " is the best, with rating = " + str(the_best_movie[0]))
