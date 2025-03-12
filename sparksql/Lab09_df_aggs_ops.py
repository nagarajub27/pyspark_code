from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main():
    spark = SparkSession.builder.appName("Lab07").master("local").getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    dftxn = spark.read.format("csv") \
            .option("delimiter" , ",") \
            .option("inferSchema",True) \
            .load("file:/home/hduser/hive/data/txns") \
            .toDF("txnid","txndate","custid","amount","category","product","city","state","paymenttype")
    
    #select state, max("amount") from txn group by state         
    dftxn.groupBy("state").max("amount").show()
    
    #select state,city, max("amount") from txn group by state,city         
    df2 = dftxn.groupBy("state","city").max("amount")
    
    #select state,city,max("amount") as maxsalesamount,min("amount") as minsalesamount,sum("amount") as totalsales,
    #count("txnid") from txn group by state, city
    
    dftxn.groupBy("state", "city") \
         .agg(max("amount").alias("maxsales"), \
         min("amount").alias("minsales"), \
         round(sum("amount"),2).alias("salesamount"), \
         count("txnid").alias("transcount")) \
         .show()
         
    #select state,city,max("amount") as maxsalesamount,min("amount") as minsalesamount,sum("amount") as totalsales,
    #count("txnid") from txn group by state, city order by state desc,city asc
         
    dftxn.groupBy("state", "city") \
         .agg(max("amount").alias("maxsales"), \
              min("amount").alias("minsales"), \
              round(sum("amount"),2).alias("salesamount"), \
              count("txnid").alias("transcount")) \
         .orderBy(desc("state"),asc("city")) \
         .show()
     
    
    #select state,city,max("amount") as maxsalesamount,min("amount") as minsalesamount,sum("amount") as totalsales,
    #count("txnid") from txn group by state, city order by state,city
         
    dftxn.groupBy("state", "city") \
         .agg(max("amount").alias("maxsales"), \
              min("amount").alias("minsales"), \
              round(sum("amount"),2).alias("salesamount"), \
              count("txnid").alias("transcount")) \
         .orderBy("state","city") \
         .show()
   
   #select state,city,max("amount") as maxsalesamount,min("amount") as minsalesamount,sum("amount") as totalsales,
   #count("txnid") from txn group by state, city having count("txnid") < 500 order by state,city
   
    dftxn.groupBy("state", "city") \
         .agg(max("amount").alias("maxsales"), \
              min("amount").alias("minsales"), \
              round(sum("amount"),2).alias("salesamount"), \
              count("txnid").alias("transcount")) \
         .orderBy("state","city").filter("transcount < 500") \
         .show()
    
        
    
    
main()
