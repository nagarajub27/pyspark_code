from pyspark.sql import SparkSession


def main():
    
    spark = SparkSession.builder.appName("Lab04-rddtodf").master("local").getOrCreate()
    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    
    lst = [("Raja",24),("Kumar",30),("Naveen",29),("Praveen",31)]
    
    rdd = sc.parallelize(lst)
    
    df = rdd.toDF(["UserName","Userage"])
    
    df.show()
    df.printSchema()
    
    
    
    
main()