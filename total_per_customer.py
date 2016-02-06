"""Exercise from the Udemy course - calculate total amount spent per customer.
Example data (CustomerID, ProductID, Amount):
44,8602,37.19
35,5368,65.89
2,3391,40.64
"""
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('TotalPerCustomer')
sc = SparkContext(conf=conf)

lines = sc.textFile('c:/SparkCourse/customer-orders.csv')
parsed_lines = lines.map(lambda x: (int(x.split(',')[0]), float(x.split(',')[2])))
# CustomerID, Amount

total_per_customer = parsed_lines.reduceByKey(lambda x,y: x+y)
# CustomerID_grouped, Amount_sum

sorted_totals = total_per_customer.map(lambda x_y: (x_y[1], x_y[0])).sortByKey()
# Amount_sum, CustomerID_grouped / ordered by 1

result = sorted_totals.collect()
for line in result:
	print("{} : {:.2f}".format(line[1], line[0]))
