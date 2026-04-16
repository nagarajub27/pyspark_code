import os
from pyspark import SparkContext

# Set Python executable path for Spark workers on Windows
#os.environ['PYSPARK_PYTHON'] = r'd:\Pyspark-2026\.venv\Scripts\python.exe'
#os.environ['PYSPARK_DRIVER_PYTHON'] = r'd:\Pyspark-2026\.
# venv\Scripts\python.exe'

def main():

    sc = SparkContext(master = 'local[1]',appName = 'lab1')
    print(sc)

    rdd = sc.parallelize([12,20,30,40,50,60,70,80])
    print('list: ',rdd.collect())
    print('count:', rdd.count())
    print('sum:', rdd.sum())
    print('maximum:', rdd.max())
    print('minimum:', rdd.min())
    print('first element:',rdd.first())
    print('take 3 elements:',rdd.take(3))
    print('get numner of partitions:', rdd.getNumPartitions())
    print('foreach print:', rdd.foreach(print))

main()