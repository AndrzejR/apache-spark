"""In the Marvel-Graph.txt data there are cooccurences of one hero and others.
When thinking what could be done with this data I started wondering, are those cooccurences distinct?
I.e., isn't it overcounting cases when one hero co-occured with another multiple times?
Why not just check it with spark?

Well, it's not so obvious... How to reduce the lists of cooccurences?
One way is apparently to make dictionaries with counts out of them and merge them.
Merging dictionaries is both commutative and associative, so it's fine for reduction.
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('AreOccurencesDistinct')
sc = SparkContext(conf=conf)

def listify_lines(line):
	tokens = line.split() # the line needs to be parsed, make sure the types are fine
	cooccurences = []
	for token in tokens[1:]:
		cooccurences.append(token)
	return (tokens[0], cooccurences) # make sure it returns what it's supposed to return... (not line[0]...)

# the function for reduce must be commutative and associative - just adding lists apparently won't work
def merge_dicts(d1, d2):
	result = {}
	for k,v in d1.items():
		result[k] = v + d2.get(k,0)
	return result

def dict_list(a_list):
	result = {}
	for i in a_list:
		result[i] = result.get(i, 0) + 1
	return result

lines = sc.textFile('c:/SparkCourse/Marvel-Graph.txt')
# SH_ID, Occ1, Occ2, ..., OccN

listified_lines = lines.map(listify_lines)
# SH_ID, [Occ1, Occ2, ..., OccN]

dict_lines = listified_lines.mapValues(dict_list)
# SH_ID, {Occ1:count, Occ2:count, ...}

reduced_lines = dict_lines.reduceByKey(merge_dicts)
# SH_ID_grouped, {Occ1:count_all, Occ2:count_all, ...}

reduced_lines_sorted_lists = reduced_lines.mapValues(lambda x: sorted(x.items(), key=lambda x: x[1], reverse=True))
# SH_ID_grouped, [(OccX, count_all_max), (OccX, count_all_max-1), ...]

rlsl_filtered = reduced_lines_sorted_lists.filter(lambda x: len(x)>1 and len(x[1])>1)

rds_max_distinct_occ = rlsl_filtered.map(lambda x: (x[1][0][1], x[0], x[1])).sortByKey(ascending=False)
# (OccX, count_all_max), SH_ID_grouped, [(OccX, count_all_max), (OccX, count_all_max-1), ...] order by 1

print(rds_max_distinct_occ.first())
