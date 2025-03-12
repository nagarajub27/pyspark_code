from pyspark.sql import SparkSession


def main():
    
    spark = SparkSession.builder.appName("Lab03-Movie").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    
    #By default, delimiter is comma
    df = spark.read.format("csv") \
        .option("delimiter","$") \
        .option("header",True) \
        .option("inferschema",True) \
        .load("file:/home/hduser/sparkdata/movies.txt")
    
    df.show()
    
    df.printSchema()
    


main()