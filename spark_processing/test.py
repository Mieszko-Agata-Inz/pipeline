import pyspark

sc = pyspark.SparkContext('local[*]')
big_list = range(10000)
rdd = sc.parallelize(big_list, 2)
odds = rdd.filter(lambda x: x % 2 != 0)
print(odds.take(5))
print(sc.version)