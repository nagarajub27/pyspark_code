from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main():
    spark = SparkSession.builder.appName("Lab12").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set("spark.sql.shuffle.partitions",2)
    
    
    df = spark.read.format("csv").option("inferschema",True).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")

    df1 = df.groupBy("prof").count()
    
    df.createOrReplaceTempView("tblcustomer")
    
    df2 = spark.sql("select prof,count(custid) from tblcustomer group by prof")
    
    # No shuffling - 1 partition
    df.write.format("csv").save("file:/home/hduser/sparkdata/customerbyprof")
    
    # Due to shuffling, default number of partition is 200
    df2.write.format("csv").save("file:/home/hduser/sparkdata/customerbyprof")
    
    # write as single partition
    df2.coalesce(1).write.format("csv").save("file:/home/hduser/sparkdata/customerbyprof")
    
    #Append,Overwrite,ErrorIfexist,Ignore 
    
    #overwrite
    df2.coalesce(1).write.mode("overwrite").format("csv").save("file:/home/hduser/sparkdata/customerbyprof")
    #or
    #df2.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").save("file:/home/hduser/sparkdata/customerbyprof")
    
    #append
    #df2.coalesce(1).write.mode(SaveMode.Append).format("csv").save("file:/home/hduser/sparkdata/customerbyprof")
    #or
    df2.coalesce(1).write.mode("append").format("csv").save("file:/home/hduser/sparkdata/customerbyprof")
    
    df3 = df2.coalesce(1)
    
    #csv,json,parquet(default),orc
    
    df3.write.mode("append").format("json").save("file:/home/hduser/sparkdata/customerbyprofjson")
    
    df3.write.mode("append").format("orc").save("file:/home/hduser/sparkdata/customerbyproforc")
    
    df3.write.mode("append").format("parquet").save("file:/home/hduser/sparkdata/customerbyprofparquet")


main()