from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main():
    spark = SparkSession.builder.appName("Lab12").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("inferSchema", True) \
        .load("file:/home/hduser/hive/data/custs") \
        .toDF("custid", "fname", "lname", "age", "prof")
        
    df.createOrReplaceTempView("tblcustomer")
    
    spark.sql("select custid,fname from tblcustomer limit 10").show(truncate=False)
    
    df.createOrReplaceGlobalTempView("tblcustomer_global")
    
    spark.sql("select custid,fname from global_temp.tblcustomer_global limit 10").show()
    
    spark1 = spark.newSession()
    
    spark.sql("select custid,fname from global_temp.tblcustomer_global limit 10").show()
    
    spark.catalog.listTables("default").show()
     
    spark.catalog.listDatabases().show()
    
    spark1.catalog.listDatabases().show()    

main()