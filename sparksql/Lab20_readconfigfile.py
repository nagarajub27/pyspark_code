from pyspark.sql import SparkSession

#09-Feb-2022

def getconfiginfo(configfile):
    
    separator = "="
    props = {}
    with open(configfile) as f:

        for line in f:
            if separator in line:
                name, value = line.split(separator, 1)
                props[name.strip()] = value.strip()
        
    
    return props


prop = getconfiginfo("/home/hduser/workspacespark/Inceptezpyspark-27/config.properties")


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
     .option("url",prop.get("pg_jdbcurl")) \
     .option("user",prop.get("pg_username")) \
     .option("password",prop.get("pg_password")) \
     .option("dbtable",prop.get("pg_table")) \
     .option("driver",prop.get("pg_driver")).load()
      
    return df 


def readtransdata(spark):
    df = spark.read.format("jdbc") \
     .option("url",prop.get("mysql_jdbcurl")) \
     .option("user",prop.get("mysql_username")) \
     .option("password",prop.get("mysql_password")) \
     .option("dbtable",prop.get("mysql_table")) \
     .option("driver",prop.get("mysql_driver")).load()
      
    return df







  

