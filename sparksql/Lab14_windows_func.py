from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from pyspark.sql.functions import *


def main():
    spark = SparkSession.builder.appName("Lab12").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.format("csv").option("inferschema",True).load("file:/home/hduser/empdata").toDF("dept","empid","salary")
     
    df.createOrReplaceTempView("tblemp")
    
    spark.sql("select empid, dept, salary, row_number() over (PARTITION BY dept ORDER BY salary) as row_num from tblemp").show() 
  
    """   
    winspec = Window.partitionBy("dept").orderBy(desc("salary"))
    
    df3 = df.withColumn("row_num", row_number().over(winspec))
     
    df3.show()
    
     
    df4 = df3.withColumn("rank",rank().over(winspec))
     
    winspec1 = Window.partitionBy("dept").orderBy(desc("salary"))
     
    df3.withColumn("denserank",dense_rank().over(winspec1)).show()
     
    df4.withColumn("ntile",ntile(3).over(winspec1)).show()
    """   

main()