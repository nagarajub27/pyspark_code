from pyspark import SparkContext
from pyspark.sql import SQLContext


def main():
    sc = SparkContext(master="local", appName="Lab02-SQL")
    sc.setLogLevel("ERROR")
    
    sqlc = SQLContext(sc)
    
    df = sqlc.read.format("csv").option("inferSchema", True) \
    .load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
    
    
    df.show()
    
    #To get the structure(columnname,datatype) of a dataframe
    df.printSchema()
        
main()