'''Calculate the sum of monthly precipitation per station from the 1800 weather dataset.
Example lines:
ITE00100554,18000101,TMAX,-75,,,E,
ITE00100554,18000101,TMIN,-148,,,E,
GM000010962,18000101,PRCP,0,,,E,
'''
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('SumOfPrecipitation')
sc = SparkContext(conf=conf)

def parse_line(line):
	split = line.split(',')
	station_id = split[0]
	month = int(split[1][4:6])
	measure_type = split[2]
	measure_value = int(split[3]) # needs to be cast, otherwise gets treated as a string

	# the new stuff, how to make complex keys for a rdd?
	return ((station_id, month), measure_type, measure_value)

lines = sc.textFile('c:/SparkCourse/1800.csv')
parsed_lines = lines.map(parse_line)

# the new from the course: filter
filtered_prcp = parsed_lines.filter(lambda x: 'PRCP' in x[1]).map(lambda x: (x[0], x[2]))
sum_by_station_month = filtered_prcp.reduceByKey(lambda x,y: x+y) # think, what exactly do x and y refer to?

result = sum_by_station_month.collect()

for line in sorted(result, key=lambda x: x[0][1]):
	print(line)
