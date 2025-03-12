from pyspark.sql import SparkSession


def main():
    
    spark = SparkSession.builder.appName("Lab18").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    """
        windows -> Preferences -> Pydev -> python-interperter
        To run external jar dependency program in eclipse ide, add below env variable in python-interperter
        
        PYSPARK_SUBMIT_ARGS 
        --master local --jars /home/hduser/install/mysql-connector-java.jar,/home/hduser/Downloads/postgresql-42.3.2.jar pyspark-shell
            
    """
    
    print("********************* Start Retail data ETL Process *****************")
    print("Step 1: Read customer data from postgres")
    dfcustomer = readcustomerdata(spark)
     
    print("Step 2: Read transaction data from mysql")
    dftrans = readtransdata(spark)
     
    print("Step 3: Register dataframe as view for both dataframes as tbltxn and tblcust")
    dfcustomer.createOrReplaceTempView("tblcust")
     
    dftrans.createOrReplaceTempView("tbltxn")
     
    print("Step 4: Join 2 dataset by writing join query based on custid column")
    dfdata = spark.sql("select state, count(txnid) as `total trans` from tbltxn t join tblcust c on t.custid = c.custid where c.prof = 'Pilot' group by state")
     
     
    print("Step 5: Write the output in local filesystem as json format")
    dfdata.coalesce(1).write.format("json").mode("overwrite").save("file:/home/hduser/pilotstatedata")
     
    print("********************* Completed Retail data ETL Process *****************")
  
    

main()


def readcustomerdata(spark):
    df = spark.read.format("jdbc") \
     .option("url","jdbc:postgresql://localhost/retail") \
     .option("user","hduser") \
     .option("password","hduser") \
     .option("dbtable","tblcustomer") \
     .option("driver","org.postgresql.Driver").load()
      
    return df 


def readtransdata(spark):
    df = spark.read.format("jdbc") \
     .option("url","jdbc:mysql://localhost/custdb") \
     .option("user","root") \
     .option("password","Root123$") \
     .option("dbtable","tbltrans") \
     .option("driver","com.mysql.cj.jdbc.Driver").load()
      
    return df 





  

