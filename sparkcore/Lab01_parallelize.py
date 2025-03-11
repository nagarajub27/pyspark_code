
from pyspark import SparkContext

#conf = SparkConf().setMaster("local").setAppName("Lab01")

#sc = SparkContext(conf=conf)

sc = SparkContext(master="local", appName="Lab01")

rdd = sc.parallelize([10,20,30,15,26,80])



print(f"Sum of Numbers: {rdd.sum()}")

print(f"Max of Numbers: {rdd.max()}")

print(f"Min of Numbers: {rdd.min()}")

print(f"Max of Numbers: {rdd.count()}")

print(f"First Element: {rdd.first()}")

lst = rdd.take(3)

print(lst)

m = rdd.mean()

print(f"Mean of Numbers: {m}")

rdd.foreach(print)

print("======================")

rdd1 = rdd.map(lambda x: x + 5)

rdd2 = rdd1.filter(lambda x: x % 2 != 0)

rdd2.foreach(print)

# parallelize -> map -> filter -> print


def add(a, b):
    print(f"sum of a + b = {a + b}")

add(10, 20)