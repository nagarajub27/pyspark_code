from pyspark.sql import SparkSession
import os

def main():
    
    spark = SparkSession.builder.appName("Lab12").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    
    
    
    """
        windows -> Preferences -> Pydev -> python-interperter
        To run external jar dependency program in eclipse ide, add below env variable in python-interperter
        
        PYSPARK_SUBMIT_ARGS --master local --jars /home/hduser/install/mysql-connector-java.jar pyspark-shell
            
    """

    
    df = spark.read.format("jdbc").option("url","jdbc:mysql://localhost/custdb").option("user","root").option("password","Root123$").option("dbtable","customer").option("driver","com.mysql.cj.jdbc.Driver").load()
    
    df.show()
    
    
     
    df.select("custid","city","age").show()
     
    df.createOrReplaceTempView("tblcustomer")
     
    spark.sql("select * from tblcustomer where transactamt > 10000").show()
     
     
    #pyspark --jars file:/home/hduser/install/mysql-connector-java.jar

    #spark-submit --master local[*] --jars file:/home/hduser/install/mysql-connector-java.jar file:/home/hduser/workspacespark/Inceptezpyspark-27/sparksql/Lab17_jdbc_operations.py
    
main()

