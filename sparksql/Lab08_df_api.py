from pyspark.sql import SparkSession,Row
from pyspark.sql.types import * 

 

def main():
    spark = SparkSession.builder.appName("Lab07").master("local").getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    dftxn = spark.read.format("csv") \
            .option("delimiter" , ",") \
            .option("inferSchema",True) \
            .load("file:/home/hduser/hive/data/txns") \
            .toDF("txnid","txndate","custid","amount","category","product","city","state","paymenttype")
    
    dftxn.filter(dftxn["state"] == "Texas").show()
    
    dftxn.filter(dftxn.state == "Texas").show()
    
    dftxn.filter("state = 'Texas'").show()
    
    texastranscount = dftxn.filter("state = 'Texas'").count()
    
    print(f"Total Transaction in Texas: {texastranscount}")
    
    
    dftxn.filter("state = 'Texas' or state = 'California'").show()
    
    dftxn.filter("paymenttype='cash' and (state = 'Texas' or state = 'California')").show()
    
    dftxn.where("paymenttype='cash' and (state = 'Texas' or state = 'California')").show()
    
    #select distinct state from txns
     
    dftxn.select("state").distinct().show()
    
    #select amount,product,city,state from txns
     
    dftxn.select("amount","product","city","state").show()
    
    
    dftxn5 = dftxn.select("amount","product","city","state")
    
    dftxn5.printSchema()
    
    #duplicates
    dftxn.select("city","state").show()
     
    #remove duplicates
    #select distinct city,state from txn where state = 'California'
    dftxn.filter("state = 'California'").select("city","state").distinct().show()
     
     #select distinct city as txncity,state as txnstate from txn where state = 'California'
    dftxn.filter("state = 'California'") \
    .select(dftxn["city"].alias("txncity"),dftxn["state"].alias("txnstate")) \
    .distinct().show()
    
    
    
    
    
    
    
    
    
main()