from pyspark.sql import SparkSession
from pyspark.sql.types import *

def main():
    spark = SparkSession.builder.appName("Lab07").master("local").getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    custid = StructField("Custid",IntegerType(),True)
      
    fname = StructField("Fname",StringType(),True)
      
    lname = StructField("Lname",StringType(),True)
      
    age = StructField("Age",IntegerType(),True)
      
    prof = StructField("Profession",StringType(),True)
    
    
    custschema = StructType([custid,fname,lname,age,prof])
    
    df = spark.read.format("csv").schema(custschema).load("file:/home/hduser/hive/data/custs")
     
    df.show()
    
    df.printSchema()
    
    
    
main()