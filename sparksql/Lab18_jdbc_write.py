from pyspark.sql import SparkSession


def main():
    
    spark = SparkSession.builder.appName("Lab18").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    """
        windows -> Preferences -> Pydev -> python-interperter
        To run external jar dependency program in eclipse ide, add below env variable in python-interperter
        
        PYSPARK_SUBMIT_ARGS --master local --jars /home/hduser/install/mysql-connector-java.jar,/home/hduser/Downloads/postgresql-42.3.2.jar pyspark-shell
            
    """
    
    #Read from csv file
    custdf = spark.read.format("csv").option("inferschema",True).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
     
    #Load data into mysql
    custdf.write.format("jdbc") \
     .mode("overwrite") \
     .option("url","jdbc:mysql://localhost/custdb") \
     .option("user","root") \
     .option("password","Root123$") \
     .option("dbtable","tblcustomer") \
     .option("driver","com.mysql.cj.jdbc.Driver") \
     .save()
     
    print("Data successfully written into mysql databases")
     
     
    custdf.write.format("jdbc") \
     .mode("overwrite")\
     .option("url","jdbc:postgresql://localhost/retail") \
     .option("user","hduser") \
     .option("password","hduser") \
     .option("dbtable","tblcustomer") \
     .option("driver","org.postgresql.Driver") \
     .save()
     
    print("Data successfully written into postgres databases")
    
    #pyspark --jars file:/home/hduser/install/mysql-connector-java.jar,file:/home/hduser/Downloads/postgresql-42.3.2.jar
    

main()