from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def main():
    spark = SparkSession.builder.appName("Lab12").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    empschema = StructType([StructField("_corrupt_record",StringType(),True),StructField("address",StructType([StructField("city",StringType(),True), 
         StructField("state",StringType(),True), StructField("zipcode",StringType(),True)]),True), 
         StructField("designation",StringType(),True), StructField("empid",LongType(),True), StructField("empname",StringType(),True), 
         StructField("skills",ArrayType(StringType(),True),True)])
    
    df = spark.read.schema(empschema).format("json").load("file:/home/hduser/sparkdata/empdetails.json")
     
    df.printSchema()
    
    df.select(df["empid"],df["empname"],df["designation"]).show()
     
    #skills - Array
    df.select(df["skills"][0],df["skills"][1]).show()

    #address - Struct
    df.select(df["address.zipcode"],df["address.city"],df["address.state"]).show()
     
    df.select(df["empid"],df["empname"],df["designation"],df["skills"][0],df["skills"][1],df["skills"][2],df["address.zipcode"],df["address.city"],df["address.state"]).show()

    df.createOrReplaceTempView("tblemployee")
     
    spark.sql("select empid,empname,designation,skills[0],skills[1],skills[2],address.city,address.state,address.zipcode from tblemployee").show()
     
    #convert array into a row
    df1 = df.select(df["empid"],df["empname"],df["designation"],explode(df["skills"]).alias("skillname"))
     
    df1.show()
     
    spark.sql("select empid,empname,designation,explode(skills),address.city,address.state,address.zipcode from tblemployee").show()
    
    df2 = spark.read.schema(empschema).format("json").option("multiline",True).load("file:/home/hduser/sparkdata/empdetails1.json")
     
    """
      There are 3 typical read modes and the default read mode is permissive.
        
            permissive — All fields are set to null and corrupted records are placed in a string column called _corrupt_record.
            dropMalformed — Drops all rows containing corrupt records.
            failFast — Fails when corrupt records are encountered.
    """
     
    spark.read.format("json").option("mode","permissive").load("file:/home/hduser/sparkdata/empdetails.json").show()
     
    spark.read.format("json").option("mode","dropMalformed").load("file:/home/hduser/sparkdata/empdetails.json").show()
     
    spark.read.format("json").option("mode","failFast").load("file:/home/hduser/sparkdata/empdetails.json").show()
    

main()
    