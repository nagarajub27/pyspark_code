from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

def main():
    spark = SparkSession.builder.appName("Lab05").master("local").getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    rdd = sc.textFile("file:/home/hduser/hive/data/custs")
    
    rdd1 = rdd.map(lambda x : x.split(","))
    
    rdd2 = rdd1.filter(lambda x : len(x) == 5)
    
    rdd3 = rdd2.map(lambda x : Row(int(x[0]),x[1],x[2],int(x[3]),x[4]))
    
    custid = StructField("Custid",IntegerType(),True)
      
    fname = StructField("Fname",StringType(),True)
      
    lname = StructField("Lname",StringType(),True)
      
    age = StructField("Age",IntegerType(),True)
      
    prof = StructField("Profession",StringType(),True)
    
    
    schema = StructType([custid,fname,lname,age,prof])
    
    df = spark.createDataFrame(rdd3, schema)
    
    df.show()
    
    df.printSchema()
    
    
    schema2 = df.schema
    
    print(schema2)
    
    
    
main()
