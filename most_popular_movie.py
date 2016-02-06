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
parse_lines = lines.map(lambda x: int(x.split()[1]))
# MovieID

prep_movies = parse_lines.map(lambda x: (x,1))
# MovieID, 1

movie_rating_counts = prep_movies.reduceByKey(lambda x,y: x+y)
# MovieID_grouped, Count

sorted_mrc = movie_rating_counts.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

the_most_popular_movie = sorted_mrc.first()

print("MovieID: " + str(the_most_popular_movie[1]) + " is the most popular, with " + str(the_most_popular_movie[0]) + " ratings.")
