from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType,StringType

#10-Feb-2022

def main():
    
    spark = SparkSession.builder.appName("Lab23-udf").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.format("csv").option("inferschema",True).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
    
    getdis = udf(lambda x: getdiscount(x), IntegerType())
    
    df.withColumn("discount", getdis(df["age"])).show()
    
    df.select(df["age"],getdis(df["age"])).show()
    
    fnfullname = udf(lambda x,y: getfullname(x, y),StringType())
    
    df.withColumn("fullname",fnfullname(df["fname"],df["lname"])).show()
    
    #If udf function need to use in sql then we need to register
    spark.udf.register("fndiscount",lambda x: getdiscount(x), IntegerType())
    
    df.createTempView("tblcust")
    
    spark.sql("select age,fndiscount(age) as discount from tblcust").show()
    
    
    
def getdiscount(age):
    if age < 10:
        return 5
    elif age < 20:
        return 10
    elif age  < 40:
        return 15
    elif age < 60:
        return 20
    else:
        return 25

def getfullname(firstname,lastname):
    return firstname + " -" + lastname
    
    
main()